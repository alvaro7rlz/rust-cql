#[macro_use]
extern crate cql;
extern crate eventual;

use std::thread;
use eventual::Async;

macro_rules! try_test(
    ($call: expr, $msg: expr) => {
        match $call {
            Ok(val) => val,
            Err(ref err) => panic!("Test failed at library call: {}", err.description())
        };
    }
);

fn async_example() {
	println!("Connecting ...!");
    let mut client = try_test!(cql::connect_async("127.0.0.1", 9042, None), "Error connecting to server at 127.0.0.1:9042");
    println!("Connected with CQL binary version v{}", client.version);

    let q = "create keyspace rust with replication = {'class': 'SimpleStrategy', 'replication_factor':1}";
    println!("cql::Query: {}", q);
    let future = client.exec_query(q, cql::Consistency::One);
    future.receive(|cqlr| {
        println!("Result: {:?}", cqlr);  
    });
    thread::sleep_ms(4000)

}

fn main() {
	async_example();
}

