//! Bearer tokens: one for every `chron.v1.Chron` call, optionally a second
//! for the administrative ones.

use std::sync::Arc;

use subtle::ConstantTimeEq;
use tonic::metadata::MetadataMap;
use tonic::service::Interceptor;
use tonic::{Request, Status};

/// The token after `Bearer ` (any case) in `authorization`, if any.
pub fn bearer(meta: &MetadataMap) -> Option<&str> {
    let v = meta.get("authorization")?.to_str().ok()?;
    let (scheme, rest) = v.split_at_checked(7)?;
    scheme
        .eq_ignore_ascii_case("bearer ")
        .then(|| rest.trim_start())
}

/// Constant time for every wrong token of the right length.
pub fn matches(got: &str, want: &[u8]) -> bool {
    bool::from(got.as_bytes().ct_eq(want))
}

/// Refuses any call that does not carry one of the configured tokens.
/// With none configured every call passes.
#[derive(Clone)]
pub struct BearerAuth {
    want: Arc<[Arc<[u8]>]>,
}

impl BearerAuth {
    pub fn new(tokens: &[&str]) -> BearerAuth {
        BearerAuth {
            want: tokens.iter().map(|t| Arc::from(t.as_bytes())).collect(),
        }
    }
}

impl Interceptor for BearerAuth {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        if self.want.is_empty() {
            return Ok(req);
        }
        match bearer(req.metadata()) {
            Some(t) if self.want.iter().any(|w| matches(t, w)) => Ok(req),
            _ => Err(Status::unauthenticated("missing or wrong bearer token")),
        }
    }
}
