extern crate cql;
extern crate eventual;
extern crate mio;

use std::borrow::Cow;
use std::io::Write;
use std::thread;
use eventual::*;

#[macro_use]
macro_rules! assert_response(
    ($resp:expr) => (
        if match $resp.opcode { cql::OpcodeResponse::OpcodeError => true, _ => false } {
            panic!("Test failed at assertion: {}",
                match $resp.body { cql::CqlResponseBody::ResponseError(_, message) => message, _ => Cow::Borrowed("Ooops!")});
        }
    );
);

macro_rules! try_test(
    ($call: expr, $msg: expr) => {
        match $call {
            Ok(val) => val,
            Err(ref err) => panic!("Test failed at library call: {}", err.description())
        };
    }
);

pub fn to_hex_string(bytes: &Vec<u8>) -> String {
  let strs: Vec<String> = bytes.iter()
                               .map(|b| format!("{:02X}", b))
                               .collect();
  strs.connect(" ")
}

fn main() {
    test_client();
}

fn test_client() {
    println!("Connecting ...!");
    let mut client = try_test!(cql::connect("127.0.0.1:9042".parse().ok().expect("Couldn't parse address"),None), "Error connecting to server at 127.0.0.1:9042");
    println!("Connected with CQL binary version v{}", client.version);

    let params = vec![cql::CqlVarchar(Some((Cow::Borrowed("TOPOLOGY_CHANGE")))), 
                                            cql::CqlVarchar(Some((Cow::Borrowed("STATUS_CHANGE")))) ];
    //thread::sleep_ms(2000); //Sleep because cassandra may not be ready for register
    let future = client.send_register(params);
    let response = try_test!(future.await().unwrap(),"Error sending register to events");
    //assert_response!(response);
    println!("Result: {:?} \n", response);
}
