package mobilegen

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Wire uses wire_package before java_package, and never Java outer classes.
type kotlinNames map[string]string

func resolveKotlinNames(files *protoregistry.Files) (kotlinNames, error) {
	names := make(kotlinNames)
	types := dynamicpb.NewTypes(files)
	var failure error
	files.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		pkg := javaPackage(file)
		encoded, err := proto.Marshal(fileOptions(file))
		if err != nil {
			failure = fmt.Errorf("%s: encode options: %w", file.Path(), err)
			return false
		}
		var options descriptorpb.FileOptions
		if err := (proto.UnmarshalOptions{Resolver: types}).Unmarshal(encoded, &options); err != nil {
			failure = fmt.Errorf("%s: resolve options: %w", file.Path(), err)
			return false
		}
		options.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			if field.FullName() == "wire.wire_package" && field.Kind() == protoreflect.StringKind {
				pkg = value.String()
			}
			return true
		})
		if !validPackage(pkg) {
			failure = fmt.Errorf("%s: invalid Wire Kotlin package %q", file.Path(), pkg)
			return false
		}
		names[file.Path()] = pkg
		return true
	})
	return names, failure
}

func (names kotlinNames) message(message protoreflect.MessageDescriptor) (string, string) {
	// These messages have built-in Kotlin representations in Wire 5.4. Their
	// nullable adapters preserve wrapper and JSON-value presence on the wire.
	switch message.FullName() {
	case "google.protobuf.Any":
		return "com.squareup.wire.AnyMessage", "com.squareup.wire.AnyMessage.ADAPTER"
	case "google.protobuf.Empty":
		return "kotlin.Unit", "com.squareup.wire.ProtoAdapter.EMPTY"
	case "google.protobuf.Timestamp":
		return "com.squareup.wire.Instant", "com.squareup.wire.ProtoAdapter.INSTANT"
	case "google.protobuf.Duration":
		return "com.squareup.wire.Duration", "com.squareup.wire.ProtoAdapter.DURATION"
	case "google.protobuf.Struct":
		return "kotlin.collections.Map<kotlin.String, *>?", "com.squareup.wire.ProtoAdapter.STRUCT_MAP"
	case "google.protobuf.Value":
		return "kotlin.Any?", "com.squareup.wire.ProtoAdapter.STRUCT_VALUE"
	case "google.protobuf.ListValue":
		return "kotlin.collections.List<*>?", "com.squareup.wire.ProtoAdapter.STRUCT_LIST"
	case "google.protobuf.DoubleValue":
		return "kotlin.Double?", "com.squareup.wire.ProtoAdapter.DOUBLE_VALUE"
	case "google.protobuf.FloatValue":
		return "kotlin.Float?", "com.squareup.wire.ProtoAdapter.FLOAT_VALUE"
	case "google.protobuf.Int64Value":
		return "kotlin.Long?", "com.squareup.wire.ProtoAdapter.INT64_VALUE"
	case "google.protobuf.UInt64Value":
		return "kotlin.Long?", "com.squareup.wire.ProtoAdapter.UINT64_VALUE"
	case "google.protobuf.Int32Value":
		return "kotlin.Int?", "com.squareup.wire.ProtoAdapter.INT32_VALUE"
	case "google.protobuf.UInt32Value":
		return "kotlin.Int?", "com.squareup.wire.ProtoAdapter.UINT32_VALUE"
	case "google.protobuf.BoolValue":
		return "kotlin.Boolean?", "com.squareup.wire.ProtoAdapter.BOOL_VALUE"
	case "google.protobuf.StringValue":
		return "kotlin.String?", "com.squareup.wire.ProtoAdapter.STRING_VALUE"
	case "google.protobuf.BytesValue":
		return "okio.ByteString?", "com.squareup.wire.ProtoAdapter.BYTES_VALUE"
	}
	name := kotlinMessageName(message, names[message.ParentFile().Path()])
	return name, name + ".ADAPTER"
}
