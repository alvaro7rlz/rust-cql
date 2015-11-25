extern crate std;
extern crate num;
extern crate uuid;
extern crate eventual;

use std::collections::BTreeMap;
use std::borrow::Cow;

use super::def::*;
use super::def::OpcodeRequest::*;
use super::def::CqlRequestBody::*;
use super::def::RCErrorType::*;
use super::def::CqlResponseBody::*;
use super::serialize::CqlSerializable;
use super::reader::*;
use super::client::*;

use std::sync::Arc;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use std::thread;

pub struct Async_client {
    client: Client
}

impl Async_client{
	/*
	pub fn connect(&mut self,ip: &'static str, port: u16, creds:Option<&Vec<CowStr>>) -> eventual::Future<RCResult<Client>,()>{
		return eventual::Future::spawn(|| {connect(ip, port, creds)});
	}
	*/	
	fn new(client:Client) -> Async_client{
		Async_client{client: client}
	}

	pub fn connect(&mut self,ip: &'static str, port: u16, creds:Option<&Vec<CowStr>>) -> RCResult<Async_client>{
		let client = connect(ip, port, creds);

		match client{
			Ok(unwrapped_client) => Ok(Async_client::new(unwrapped_client)),
			Err(e) => Err(RCError::new("Unable to connect with Async_client", ReadError))
		}
	}

	/*
	pub fn exec_query(& mut self,query_str: & str, con: Consistency) ->  eventual::Future<RCResult<CqlResponse>,()>{
		eventual::Future::spawn(||{
			self.client.exec_query(query_str,con)
		});
	}
	*/
	
	/* Same as above function with lifetimes (it doesn't compile though)
	pub fn exec_query<'a>(&'a mut self,query_str: &'a str, con: Consistency) ->  eventual::Future<RCResult<CqlResponse<'a>>,()>{
		eventual::Future::spawn(||{
			self.client.exec_query(query_str,con)
		});
	}
	*/
	
	
	/*
	pub fn exec_prepared(&mut self, ps_id: CowStr, params: &[CqlValue], con: Consistency)  -> eventual::Future<RCResult<CqlResponse>,()> {
		eventual::Future::spawn(|| {
			self.foo(ps_id)
		});
	}
*/
	pub fn bar(&mut self, ps_id: CowStr, params: &[CqlValue], con: Consistency)  -> Receiver<RCResult<CqlResponse>> {
		let (tx, rx): (Sender<RCResult<CqlResponse>>, Receiver<RCResult<CqlResponse>>) = mpsc::channel();

        thread::spawn(move || {
            // The thread takes ownership over `thread_tx`
            // Each thread queues a message in the channel
            let res = CqlResponse {
            version: 1,
            flags: 1,
            stream: 1,
            opcode: OpcodeResponse::OpcodeReady,
            body: ResultVoid
        };
            tx.send(Ok(res)).unwrap();

            // Sending is a non-blocking operation, the thread will continue
            // immediately after sending its message
            println!("thread finished!");
        });

        rx

	}

	pub fn foo(&mut self, ps_id: CowStr) -> RCResult<CowStr> {
		Ok(Cow::Borrowed("ttrwe"))
	}

}