//! Resolves fully-qualified protobuf type names to Rust paths relative to the
//! module the generated code lives in, honoring `extern_path` overrides and
//! the well-known-type substitutions prost performs.
//!
//! Adapted from `protoc-gen-tonic` (neoeinstein/protoc-gen-prost, Apache-2.0).

use std::{collections::HashMap, iter};

use prost_build::Module;

use crate::util;

pub(crate) struct Resolver {
    extern_root: Node,
}

#[derive(Default, Debug)]
struct Node {
    extern_path: Option<String>,
    children: HashMap<String, Node>,
}

impl Node {
    fn insert(&mut self, fq_proto_path: &str, extern_path: String) {
        assert_eq!(".", &fq_proto_path[..1]);
        let mut path = &fq_proto_path[1..];
        let mut current = &mut *self;
        while !path.is_empty() {
            let (next, remaining) = path.split_once('.').unwrap_or((path, ""));
            path = remaining;
            current = current.children.entry(next.to_owned()).or_default();
        }
        current.extern_path = Some(extern_path);
    }

    fn resolve_extern(&self, fq_proto_path: &str) -> Option<String> {
        assert_eq!(".", &fq_proto_path[..1]);
        let mut path = &fq_proto_path[1..];
        let mut current = self;
        while !path.is_empty() {
            let (next, remaining) = path.split_once('.').unwrap_or((path, ""));
            if let Some(node) = current.children.get(next) {
                current = node;
                path = remaining;
            } else {
                let prefix = current.extern_path.as_deref()?;
                let (package, type_name) = path.rsplit_once('.').unwrap_or(("", path));
                let module = Module::from_protobuf_package_name(package);
                return Some(
                    module
                        .parts()
                        .chain(iter::once(util::to_upper_camel(type_name).as_str()))
                        .fold(prefix.to_owned(), |mut acc, next| {
                            acc.push_str("::");
                            acc.push_str(next);
                            acc
                        }),
                );
            }
        }
        current.extern_path.clone()
    }
}

impl Resolver {
    pub(crate) fn new(extern_path: Vec<(String, String)>, compile_well_known_types: bool) -> Self {
        let mut extern_root = Node::default();

        if !compile_well_known_types {
            for (proto, rust) in [
                (".google.protobuf", "::prost_types"),
                (".google.protobuf.BoolValue", "bool"),
                (
                    ".google.protobuf.BytesValue",
                    "::prost::alloc::vec::Vec<u8>",
                ),
                (".google.protobuf.DoubleValue", "f64"),
                (".google.protobuf.Empty", "()"),
                (".google.protobuf.FloatValue", "f32"),
                (".google.protobuf.Int32Value", "i32"),
                (".google.protobuf.Int64Value", "i64"),
                (
                    ".google.protobuf.StringValue",
                    "::prost::alloc::string::String",
                ),
                (".google.protobuf.UInt32Value", "u32"),
                (".google.protobuf.UInt64Value", "u64"),
            ] {
                extern_root.insert(proto, rust.to_owned());
            }
        }

        for (fq_proto_path, extern_path) in extern_path {
            extern_root.insert(&fq_proto_path, extern_path);
        }

        Self { extern_root }
    }

    /// Rust path to `to_fq` as seen from code placed in `from`'s `.ws.rs`
    /// file, which is `include!`d into the prost module — so same-package
    /// types resolve to `super::Type`.
    pub(crate) fn resolve_ident(&self, from: &Module, to_fq: &str) -> String {
        if let Some(ident) = self.extern_root.resolve_extern(to_fq) {
            return ident;
        }

        let (package, type_name) = to_fq.rsplit_once('.').unwrap();
        let to = Module::from_protobuf_package_name(package);
        let (down, prefix) = difference(from, &to);

        // +1: the generated code sits inside `<service>_ws`, one level below
        // the package module that holds the prost types.
        iter::repeat_n("super".to_owned(), down + 1)
            .chain(to.parts().skip(prefix).map(|s| s.to_owned()))
            .chain(iter::once(util::to_upper_camel(type_name)))
            .reduce(|mut l, r| {
                l.push_str("::");
                l.push_str(&r);
                l
            })
            .unwrap_or_default()
    }
}

fn difference(left: &Module, right: &Module) -> (usize, usize) {
    let mut left_parts = left.parts();
    let mut right_parts = right.parts();
    let mut prefix = 0;
    loop {
        match (left_parts.next(), right_parts.next()) {
            (Some(l), Some(r)) if l == r => prefix += 1,
            (Some(_), Some(_)) | (Some(_), None) => return (left_parts.count() + 1, prefix),
            (None, _) => return (0, prefix),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pkg: &str) -> Module {
        Module::from_protobuf_package_name(pkg)
    }

    #[test]
    fn same_package_is_super() {
        let r = Resolver::new(vec![], false);
        assert_eq!(
            r.resolve_ident(&m("echo.v1"), ".echo.v1.UnaryRequest"),
            "super::UnaryRequest"
        );
    }

    #[test]
    fn sibling_package_walks_up() {
        let r = Resolver::new(vec![], false);
        assert_eq!(
            r.resolve_ident(&m("echo.v1"), ".other.v2.Thing"),
            "super::super::super::other::v2::Thing"
        );
    }

    #[test]
    fn well_known_types_map_to_prost_types() {
        let r = Resolver::new(vec![], false);
        assert_eq!(
            r.resolve_ident(&m("echo.v1"), ".google.protobuf.Empty"),
            "()"
        );
        assert_eq!(
            r.resolve_ident(&m("echo.v1"), ".google.protobuf.Timestamp"),
            "::prost_types::Timestamp"
        );
    }

    #[test]
    fn extern_path_override() {
        let r = Resolver::new(
            vec![(".common".to_owned(), "::common_pb".to_owned())],
            false,
        );
        assert_eq!(
            r.resolve_ident(&m("echo.v1"), ".common.v1.Id"),
            "::common_pb::v1::Id"
        );
    }
}
