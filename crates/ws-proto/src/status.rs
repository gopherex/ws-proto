//! RPC status: gRPC-style code + message + details, carried on END/RST frames.

use std::fmt;

use bytes::Bytes;

use crate::transport;

/// gRPC status codes (<https://grpc.io/docs/guides/status-codes/>).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum Code {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl Code {
    /// Maps a wire value; anything outside the known range is `Unknown`.
    pub fn from_i32(v: i32) -> Code {
        use Code::*;
        match v {
            0 => Ok,
            1 => Cancelled,
            2 => Unknown,
            3 => InvalidArgument,
            4 => DeadlineExceeded,
            5 => NotFound,
            6 => AlreadyExists,
            7 => PermissionDenied,
            8 => ResourceExhausted,
            9 => FailedPrecondition,
            10 => Aborted,
            11 => OutOfRange,
            12 => Unimplemented,
            13 => Internal,
            14 => Unavailable,
            15 => DataLoss,
            16 => Unauthenticated,
            _ => Unknown,
        }
    }
}

/// A transport-level RPC status. Used as the error type of every stream
/// operation: a non-OK END or an RST from the peer surfaces as `Err(Status)`,
/// local failures (connection lost, overflow) likewise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub code: Code,
    pub message: String,
    /// Marshaled `google.protobuf.Any` payloads.
    pub details: Vec<Bytes>,
}

impl Status {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: Vec::new(),
        }
    }

    pub fn ok() -> Self {
        Self::new(Code::Ok, "")
    }

    pub fn is_ok(&self) -> bool {
        self.code == Code::Ok
    }

    pub(crate) fn to_proto(&self) -> transport::Status {
        transport::Status {
            code: self.code as i32,
            message: self.message.clone(),
            details: self.details.clone(),
        }
    }

    pub(crate) fn from_proto(p: Option<&transport::Status>) -> Self {
        match p {
            None => Self::ok(),
            Some(p) => Self {
                code: Code::from_i32(p.code),
                message: p.message.clone(),
                details: p.details.clone(),
            },
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "wsrpc: code = {:?} desc = {}", self.code, self.message)
    }
}

impl std::error::Error for Status {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_roundtrip_and_fallback() {
        for v in 0..=16 {
            assert_eq!(Code::from_i32(v) as i32, v);
        }
        assert_eq!(Code::from_i32(99), Code::Unknown);
        assert_eq!(Code::from_i32(-1), Code::Unknown);
    }

    #[test]
    fn proto_roundtrip() {
        let st = Status {
            code: Code::NotFound,
            message: "nf".into(),
            details: vec![Bytes::from_static(b"x")],
        };
        assert_eq!(Status::from_proto(Some(&st.to_proto())), st);
        assert_eq!(Status::from_proto(None), Status::ok());
    }
}
