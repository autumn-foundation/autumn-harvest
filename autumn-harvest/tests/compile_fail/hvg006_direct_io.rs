use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = std::fs::read_to_string("config.json");
    let _ = tonic::transport::Endpoint::from_shared("http://[::1]:50051");
    let _ = tokio_postgres::connect("host=localhost user=postgres", tokio_postgres::NoTls);
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

fn main() {}

