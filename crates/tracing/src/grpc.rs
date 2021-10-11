use tonic::transport::Channel;

pub type Trace = crate::tower::Trace<Channel>;
pub use crate::tower::TraceLayer;
