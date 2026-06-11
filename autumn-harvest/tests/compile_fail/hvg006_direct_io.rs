use autumn_harvest::prelude::*;

struct Command;
impl Command {
    fn new(_: &str) -> Self { Self }
}

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = std::fs::read_to_string("config.json");
    let _ = tonic::transport::Endpoint::from_shared("http://[::1]:50051");
    let _ = tokio_postgres::connect("host=localhost user=postgres", tokio_postgres::NoTls);
    
    // std::net::Ipv4Addr constructor (deterministic, allowed)
    let _ip = std::net::Ipv4Addr::new(127, 0, 0, 1);
    
    // std::net::TcpStream connect (blocked)
    let _ = std::net::TcpStream::connect("127.0.0.1:8080");
    
    // Unrelated connect call (deterministic, allowed)
    let _ = graph::connect(1, 2);

    // Custom Command::new constructor (allowed)
    let _ = Command::new("custom");
    
    // std::process::Command::new (blocked)
    let _ = std::process::Command::new("ls");
    
    Ok(())
}

mod tonic {
    pub mod transport {
        pub struct Endpoint;
        impl Endpoint {
            pub fn from_shared(_: &str) -> Self {
                Self
            }
        }
    }
}

mod tokio_postgres {
    pub struct NoTls;
    pub fn connect(_: &str, _: NoTls) {}
}

mod graph {
    pub fn connect(_a: i32, _b: i32) -> i32 {
        0
    }
}

fn main() {}


