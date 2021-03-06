
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::net::{SocketAddr,IpAddr,Ipv4Addr};
use std::error::Error;
use std::thread;
use mio::{EventLoop,EventLoopConfig, Sender, Handler};

use eventual::Async;
use util;
use def::*;
use def::TopologyChangeType::*;
use def::StatusChangeType::*;
use def::CqlResponseBody::*;
use def::CqlValue::*;
use std::time::Duration;
use node::Node;
use connection_pool::ConnectionPool;
use connection::CqlMsg;
use std::convert::AsRef;
use std::rc::Rc;
use std::boxed::Box;
use std::cell::RefCell;
use load_balancing::*;
use error::*;
use error::RCErrorType::*;
use std::sync::mpsc;

type ArcMap = Arc<RwLock<BTreeMap<IpAddr,Node>>>;

pub struct Cluster{
	// Index of the current_node we are using
	current_node:  Arc<RwLock<IpAddr>>,	
	available_nodes: ArcMap,
	unavailable_nodes: ArcMap,
	channel_cpool: Sender<CqlMsg>,
	// https://doc.rust-lang.org/error-index.html#E0038
	balancer:  Arc<RwLock<LoadBalancing+Send+Sync>>,
	balancer_sender: mpsc::Sender<()>
}


impl Cluster {

	pub fn new() -> Cluster{
		let availables 	 = Arc::new(RwLock::new(BTreeMap::new()));
		let unavailables = Arc::new(RwLock::new(BTreeMap::new()));

		//Start EventLoop<ConnectionPool>

		let mut config = EventLoopConfig::new();
			config.notify_capacity(65_536)
	        .messages_per_tick(1_024)
	        //.timer_tick(Duration::from_millis(100))
	        .timer_wheel_size(1_024)
	        .timer_capacity(65_536);

        let mut event_loop_conn_pool : EventLoop<ConnectionPool> = 
        		EventLoop::configured(config.clone()).ok().expect("Couldn't create event loop");
        let mut channel_cpool= event_loop_conn_pool.channel();


    	let balancer = Arc::new(RwLock::new(RoundRobin{index:0}));
        let current_node = Arc::new(RwLock::new(IpAddr::V4(Ipv4Addr::new(0,0,0,0))));

		//Start EventLoop<EventHandler>
        let mut event_loop : EventLoop<EventHandler> = 
        		EventLoop::configured(config).ok().expect("Couldn't create event loop");
        let event_handler_channel = event_loop.channel();
        let mut event_handler = EventHandler::new(	availables.clone(),
        										  	unavailables.clone(),
        										  	channel_cpool.clone(),
        										  	current_node.clone());

        // Only keep the event loop channel
        thread::Builder::new().name("event_handler".to_string()).spawn(move || {
        	event_loop.run(&mut event_handler).ok().expect("Failed to start event loop");
		});

        


        // We will need the event loop to register a new socket
        // but on creating the thread we borrow the even_loop.
        // So we 'give away' the connection pool and keep the channel.
        let mut connection_pool = ConnectionPool::new(event_handler_channel);

        //println!("Starting event loop...");
        // Only keep the event loop channel
        thread::Builder::new().name("connection_pool".to_string()).spawn(move || {
                event_loop_conn_pool.run(&mut connection_pool).ok().expect("Failed to start event loop");
        });


		Cluster{
			available_nodes: availables.clone(),
			unavailable_nodes: unavailables.clone(),
			channel_cpool: channel_cpool,
			current_node: Arc::new(RwLock::new(IpAddr::V4(Ipv4Addr::new(0,0,0,0)))),
			balancer: balancer,
			balancer_sender: mpsc::channel().0
		}
	}

	fn start_load_balancing(&mut self,duration:Duration){
		let availables = self.available_nodes.clone();
        let current_node = self.current_node.clone();
        let balancer = self.balancer.clone();
        let tx = 
	        util::set_interval(duration,move || {
	        	//println!("set_interval");
	        	let mut node = current_node.write().unwrap();
	        	*node = balancer.write().unwrap().select_node(&availables.read().unwrap());
	        });
	    self.balancer_sender = tx;
	}
	
	pub fn set_load_balancing(&mut self,balancer: BalancerType,duration: Duration){
		//Stop load balancer thread sending a '()' message
		self.balancer_sender.send(());
		match balancer{
			BalancerType::RoundRobin => self.balancer = Arc::new(RwLock::new(RoundRobin{index:0})),
			BalancerType::LatencyAware => self.balancer = Arc::new(RwLock::new(LatencyAware)),
		}
		self.start_load_balancing(duration);
	}

	pub fn are_available_nodes(&self) -> bool{
		self.available_nodes.read()
							.unwrap()
							.len() == 0
	}

	fn add_node(&self,ip: IpAddr) -> RCResult<CqlResponse>{
		let address = SocketAddr::new(ip,CQL_DEFAULT_PORT);
		let mut node = Node::new(address,self.channel_cpool.clone());
		node.set_channel_cpool(self.channel_cpool.clone());

		let response = {
			try_unwrap!(node.connect().await())
		};

		match response {
			Ok(_) => {
				try_unwrap!(self.available_nodes.write())
							.insert(address.ip(),node);
			}
			Err(_) =>{
				try_unwrap!(self.unavailable_nodes.write())
							.insert(address.ip(),node);
			}
		}
		response
	}

	// This operation blocks
	pub fn connect_cluster(&mut self,address: SocketAddr) -> RCResult<CqlResponse>{
		// No avaiables nodes make sure that 'tick' thread is not writing
		if self.are_available_nodes(){
			{
			let mut node = self.current_node.write().unwrap();
			*node = address.ip();
			}
			{
			self.start_load_balancing(Duration::from_secs(1));
			}
			return self.create_and_register();
		}
		else{
			return Err(RCError::new("Already connected to cluster", ClusterError)) 
		}
	}

	fn create_and_register(&mut self) -> RCResult<CqlResponse>{
		let connect_response = self.add_node(self.current_node.read().unwrap().clone());
		match connect_response{
			Ok(_) => {
				//Register the connection to get Events from Cassandra
				try_unwrap!(try_unwrap!(self.register().await()));

				//Get the currrent nodes from a system query
				let peers = try_unwrap!(try_unwrap!(self.get_peers().await()));
				let ip_nodes = try_unwrap!(parse_nodes(peers));
				self.create_nodes(ip_nodes);
			},
			Err(_) =>{
				()
			}
		}
		connect_response
	}

	

	fn create_nodes(&mut self,ips: Vec<IpAddr>){
		for ip in ips {
		    self.add_node(ip);
		}
	}


	pub fn get_peers(&mut self) -> CassFuture{
		let map = self.available_nodes
			   .read()
			   .unwrap();
		let node = map.get(&self.current_node.read().unwrap())
					   .unwrap();
		node.get_peers()
	}


	pub fn exec_query(&mut self, query_str: &str, con: Consistency) -> CassFuture {
		let map = self.available_nodes
					   .read()
					   .unwrap();
		let node = map.get(&self.current_node.read().unwrap())
					   .unwrap();
					   
		node.exec_query(query_str,con)
	}

	//This operation blocks
	pub fn prepared_statement(&mut self, query_str: &str) -> RCResult<CqlPreparedStat> {
		let map = self.available_nodes
			   .read()
			   .unwrap();
		let node = map.get(&self.current_node.read().unwrap())
					   .unwrap();
					   
		node.prepared_statement(query_str)
	}

	 pub fn exec_prepared(&mut self, preps: &Vec<u8>, params: &Vec<CqlValue>, con: Consistency) -> CassFuture{
		let map = self.available_nodes
			   .read()
			   .unwrap();
		let node = map.get(&self.current_node.read().unwrap())
					   .unwrap();
					   
		node.exec_prepared(preps,params,con)
	}

	pub fn exec_batch(&mut self, q_type: BatchType, q_vec: Vec<Query>, con: Consistency) -> CassFuture {
		let map = self.available_nodes
			   .read()
			   .unwrap();
		let node = map.get(&self.current_node.read().unwrap())
					   .unwrap();
					   
		node.exec_batch(q_type,q_vec,con)
	}



	fn register(&mut self) -> CassFuture{
		let map = self.available_nodes
			   		.read()
			   		.unwrap();
		let node = 	map.get(&self.current_node.read().unwrap())
			   			.unwrap();
		node.send_register()
	}

	// This temporal until I return some type
	pub fn show_cluster_information(&self){
		let map_availables = 
			self.available_nodes
	   			.read()
	   			.unwrap();
	   	let map_unavailables = 
	   		self.unavailable_nodes
	   			.read()
	   			.unwrap();

		println!("--------------Available nodes-----------");
		println!("Address");
		for node in map_availables.iter() {
			println!("{:?}\t",node.0);
		}
		println!("----------------------------------------");

		println!("--------------Unavailable nodes---------");
		println!("Address");
		for node in map_unavailables.iter() {
			println!("{:?}\t",node.0);
		}
		println!("----------------------------------------");
		println!("Current node: {:?}\n",self.current_node);
		println!("----------------------------------------\n");
		//println!("Current balaning strategy: {}\n",self.balancer);
		//println!("----------------------------------------\n");
	}
}

pub fn parse_nodes(response: CqlResponse) -> RCResult<Vec<IpAddr>>{
		let mut nodes = Vec::new();
		match response.body {
			ResultRows(cql_rows) => {
				if cql_rows.rows.len() > 0 {
					let rows = cql_rows.rows.clone();
					for row in rows {
						//println!("Col: {:?}",row);
						match *row.cols.get(0).unwrap() {
							CqlInet(Some(ip)) => {
								nodes.push(ip);
							},
							_ => return Err(RCError::new("Error CqlResponse contains no rows", ReadError)),
						}
					}
					Ok(nodes)
				}
				else{
					Err(RCError::new("Error CqlResponse contains no rows", ReadError))
				}
			},
			_ => Err(RCError::new("Error CqlResponse type must be ResultRows", ClusterError)),
		}
	}

struct EventHandler{
	available_nodes: ArcMap,
	unavailable_nodes: ArcMap,
	channel_cpool: Sender<CqlMsg>,
	current_node: Arc<RwLock<IpAddr>>
}

impl EventHandler{
	fn new(availables: ArcMap,unavailables: ArcMap,channel_cpool : Sender<CqlMsg>,
		   current_node: Arc<RwLock<IpAddr>>) -> EventHandler{
		EventHandler{
			available_nodes: availables,
			unavailable_nodes: unavailables,
			channel_cpool: channel_cpool,
			current_node: current_node
		}
	}
	pub fn show_cluster_information(&self){
		let map_availables = 
			self.available_nodes
	   			.read()
	   			.unwrap();
	   	let map_unavailables = 
	   		self.unavailable_nodes
	   			.read()
	   			.unwrap();
	   	println!("EventHandler::show_cluster_information");
		println!("--------------Available nodes-----------");
		println!("Address");
		for node in map_availables.iter() {
			println!("{:?}\t",node.0);
		}
		println!("----------------------------------------");

		println!("--------------Unavailable nodes---------");
		println!("Address");
		for node in map_unavailables.iter() {
			println!("{:?}\t",node.0);
		}
		println!("----------------------------------------");
	}

}

impl Handler for EventHandler {
    type Timeout = ();

    type Message = CqlEvent; 

    fn notify(&mut self, event_loop: &mut EventLoop<EventHandler>, msg: CqlEvent) {
    	println!("EventHandler::notify");
    	match msg {
    		CqlEvent::TopologyChange(change_type,socket_addr) =>{
    			let mut map = 
	    				self.available_nodes
						   	.write()
						   	.ok().expect("Can't write in available_nodes");
    			match change_type{
    				NewNode =>{
    					map.insert(socket_addr.ip(),
    					Node::new(socket_addr,self.channel_cpool.clone()));
    				},
    				RemovedNode =>{
    					map.remove(&socket_addr.ip());
    				},
    				MovedNode =>{
    					//Not sure about this.
    					map.insert(socket_addr.ip(),
    					Node::new(socket_addr,self.channel_cpool.clone()));
    				},
    				Unknown => ()
    			}
			},
			CqlEvent::StatusChange(change_type,socket_addr) =>{
				//Need for a unavailable_nodes list (down)
				let mut map_unavailable = self.unavailable_nodes
					   		.write()
					   		.ok().expect("Can't write in unavailable_nodes");
				let mut map_available = self.available_nodes
										.write()
										.ok().expect("Can't write in unavailable_nodes");
				match change_type{
					Up =>{
					   	//To-do: treat error if node doesn't exist
    					let result_node = map_unavailable.remove(&socket_addr.ip());

    					match result_node {
    					    Some(node) => {  					
		    					map_available.insert(node.get_sock_addr().ip(),node);
    						},
    					    None => println!("Node with ip {:?} wasn't found in unavailable_nodes",&socket_addr.ip()),
    					}
  
					},
					Down =>{
					   	//To-do: treat error if node doesn't exist
    					let result_node = map_available.remove(&socket_addr.ip());

    					match result_node {
    					    Some(node) => {
	    						map_unavailable.insert(node.get_sock_addr().ip(),node);
    					    },
    					    None => println!("Node with ip {:?} wasn't found in available_nodes",&socket_addr.ip()),
    					}
    					
					},
					UnknownStatus => ()
				}
			},
			CqlEvent::SchemaChange(change_type,socket_addr) =>{
				println!("Schema changes are not supported yet");
			},
			CqlEvent::UnknownEvent=> {
				println!("We've got an UnkownEvent");
			}
		}
	
   }
}