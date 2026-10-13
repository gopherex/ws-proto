use heck::ToUpperCamelCase;
use prost_build::Module;

/// `UpperCamel` → `snake_case` using prost's own module-name rules so the
/// result matches what prost would generate for the same identifier.
pub(crate) fn to_snake(s: &str) -> String {
    let as_module = Module::from_protobuf_package_name(s);
    assert_eq!(as_module.len(), 1, "unexpected `.` in name part");
    let mut parts = as_module.parts();
    parts.next().unwrap().to_owned()
}

/// Converts an identifier to an `UpperCamel` Rust type identifier.
pub(crate) fn to_upper_camel(s: &str) -> String {
    let mut ident = s.to_upper_camel_case();
    if ident == "Self" {
        ident += "_";
    }
    ident
}

/// Leading comment attached to the descriptor at `path` (see
/// `SourceCodeInfo.Location.path`), or empty.
pub(crate) fn leading_comment(file: &prost_types::FileDescriptorProto, path: &[i32]) -> String {
    file.source_code_info
        .as_ref()
        .and_then(|sci| sci.location.iter().find(|l| l.path == path))
        .map(|l| l.leading_comments().to_owned())
        .unwrap_or_default()
}
