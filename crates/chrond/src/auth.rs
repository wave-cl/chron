//! A bearer token on every `chron.v1.Chron` call.

use std::sync::Arc;

use subtle::ConstantTimeEq;
use tonic::service::Interceptor;
use tonic::{Request, Status};

/// Refuses any call that does not carry `authorization: Bearer <token>`
/// when a token is configured. The comparison takes the same time for
/// every wrong token of the right length.
#[derive(Clone)]
pub struct BearerAuth {
    want: Option<Arc<[u8]>>,
}

impl BearerAuth {
    pub fn new(token: Option<&str>) -> BearerAuth {
        BearerAuth {
            want: token.map(|t| Arc::from(t.as_bytes())),
        }
    }
}

impl Interceptor for BearerAuth {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let Some(want) = &self.want else {
            return Ok(req);
        };
        let got = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        match got {
            Some(t) if bool::from(t.as_bytes().ct_eq(want)) => Ok(req),
            _ => Err(Status::unauthenticated("missing or wrong bearer token")),
        }
    }
}
