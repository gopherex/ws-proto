package mobilegen

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"google.golang.org/protobuf/types/pluginpb"
)

func TestWireAdapterDispatch(t *testing.T) {
	t.Parallel()
	response := Generate(echoRequest(), Kotlin)
	if response.GetError() != "" {
		t.Fatal(response.GetError())
	}
	content := response.File[0].GetContent()
	for _, want := range []string{
		"input = `echo`.`v1`.`UnaryRequest`.ADAPTER",
		"output = `echo`.`v1`.`UnaryResponse`.ADAPTER",
		"input = `echo`.`v1`.`ClientStreamRequest`.ADAPTER",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("missing %s", want)
		}
	}
	for _, unwanted := range []string{".parser()", "`Echo`.", "com.google.protobuf"} {
		if strings.Contains(content, unwanted) {
			t.Errorf("Java-lite dependency in Wire client: %s", unwanted)
		}
	}
}

func TestWireRejectsKeywordMessageTypes(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"class", "when"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			request := echoRequest()
			request.ProtoFile[0].MessageType[0].Name = proto.String(name)
			request.ProtoFile[0].Service[0].Method[0].InputType = proto.String(".echo.v1." + name)
			response := Generate(request, Kotlin)
			if !strings.Contains(response.GetError(), "Wire cannot generate") || len(response.File) != 0 {
				t.Fatalf("expected actionable atomic rejection: %v", response)
			}
		})
	}
}

func TestWirePackageOverride(t *testing.T) {
	t.Parallel()
	for _, pkg := range []string{"other.when", ""} {
		t.Run(pkg, func(t *testing.T) {
			t.Parallel()
			request := wirePackageRequest(pkg)
			response := Generate(request, Kotlin)
			if response.GetError() != "" {
				t.Fatal(response.GetError())
			}
			expectedFile := "EchoServiceClient.kt"
			if pkg != "" {
				expectedFile = "other/when/" + expectedFile
			}
			if response.File[0].GetName() != expectedFile {
				t.Fatal(response.File[0].GetName())
			}
			content := response.File[0].GetContent()
			if strings.Contains(content, "`echo`.`v1`.") {
				t.Fatal("ignored wire_package")
			}
			if !strings.Contains(content, "typeName = \"echo.v1.EchoService\"") {
				t.Fatal("wire route changed with Kotlin package")
			}
		})
	}
}

func TestWireRejectsUnsafePackage(t *testing.T) {
	t.Parallel()
	for _, pkg := range []string{"../escape", "/absolute", "foo..bar", "bad;package", "a/b", "white space"} {
		response := Generate(wirePackageRequest(pkg), Kotlin)
		if response.GetError() == "" || len(response.File) != 0 {
			t.Fatalf("%q: accepted invalid package", pkg)
		}
	}
}

func wirePackageRequest(pkg string) *pluginpb.CodeGeneratorRequest {
	request := echoRequest()
	options := request.ProtoFile[0].Options
	options.JavaPackage = proto.String("ignored.java")
	wireOption := protowire.AppendTag(nil, 1087, protowire.BytesType)
	wireOption = protowire.AppendString(wireOption, pkg)
	options.ProtoReflect().SetUnknown(wireOption)
	extension := &descriptorpb.FileDescriptorProto{
		Name: proto.String("wire/extensions.proto"), Package: proto.String("wire"), Syntax: proto.String("proto2"),
		Dependency: []string{"google/protobuf/descriptor.proto"},
		Extension: []*descriptorpb.FieldDescriptorProto{{
			Name: proto.String("wire_package"), Number: proto.Int32(1087),
			Extendee: proto.String(".google.protobuf.FileOptions"),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		}},
	}
	request.ProtoFile[0].Dependency = append(request.ProtoFile[0].Dependency, extension.GetName())
	request.ProtoFile = append(request.ProtoFile, extension, protodesc.ToFileDescriptorProto(descriptorpb.File_google_protobuf_descriptor_proto))
	return request
}

func TestWireWellKnownAdapters(t *testing.T) {
	t.Parallel()
	files := []*descriptorpb.FileDescriptorProto{
		protodesc.ToFileDescriptorProto(anypb.File_google_protobuf_any_proto),
		protodesc.ToFileDescriptorProto(emptypb.File_google_protobuf_empty_proto),
		protodesc.ToFileDescriptorProto(wrapperspb.File_google_protobuf_wrappers_proto),
		protodesc.ToFileDescriptorProto(timestamppb.File_google_protobuf_timestamp_proto),
		protodesc.ToFileDescriptorProto(durationpb.File_google_protobuf_duration_proto),
		protodesc.ToFileDescriptorProto(structpb.File_google_protobuf_struct_proto),
	}
	for _, test := range []struct{ name, kotlin, adapter string }{
		{"Any", "com.squareup.wire.AnyMessage", "com.squareup.wire.AnyMessage.ADAPTER"},
		{"Empty", "kotlin.Unit", "com.squareup.wire.ProtoAdapter.EMPTY"},
		{"StringValue", "kotlin.String?", "com.squareup.wire.ProtoAdapter.STRING_VALUE"},
		{"Int32Value", "kotlin.Int?", "com.squareup.wire.ProtoAdapter.INT32_VALUE"},
		{"Timestamp", "com.squareup.wire.Instant", "com.squareup.wire.ProtoAdapter.INSTANT"},
		{"Duration", "com.squareup.wire.Duration", "com.squareup.wire.ProtoAdapter.DURATION"},
		{"Struct", "kotlin.collections.Map<kotlin.String, *>?", "com.squareup.wire.ProtoAdapter.STRUCT_MAP"},
		{"Value", "kotlin.Any?", "com.squareup.wire.ProtoAdapter.STRUCT_VALUE"},
		{"ListValue", "kotlin.collections.List<*>?", "com.squareup.wire.ProtoAdapter.STRUCT_LIST"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := echoRequest()
			for _, file := range files {
				request.ProtoFile[0].Dependency = append(request.ProtoFile[0].Dependency, file.GetName())
			}
			request.ProtoFile = append(request.ProtoFile, files...)
			request.ProtoFile[0].Service[0].Method[0].InputType = proto.String(".google.protobuf." + test.name)
			request.ProtoFile[0].Service[0].Method[0].OutputType = proto.String(".google.protobuf." + test.name)
			response := Generate(request, Kotlin)
			if response.GetError() != "" {
				t.Fatal(response.GetError())
			}
			content := response.File[0].GetContent()
			for _, want := range []string{"input = " + test.adapter, "output = " + test.adapter, "MethodInfo<" + test.kotlin + ", " + test.kotlin + ">"} {
				if !strings.Contains(content, want) {
					t.Errorf("missing %s", want)
				}
			}
		})
	}
}

func TestWireKeywordInReachableField(t *testing.T) {
	t.Parallel()
	request := echoRequest()
	file := request.ProtoFile[0]
	file.MessageType = append(file.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("when")})
	file.MessageType[0].Field = append(file.MessageType[0].Field, &descriptorpb.FieldDescriptorProto{
		Name: proto.String("nested"), Number: proto.Int32(100), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".echo.v1.when"),
	})
	response := Generate(request, Kotlin)
	if !strings.Contains(response.GetError(), "Wire cannot generate") || len(response.File) != 0 {
		t.Fatal(response.GetError())
	}
}

func TestWireSoftKeywordMessageTypes(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"data", "value", "field", "get", "set", "internal"} {
		request := echoRequest()
		request.ProtoFile[0].MessageType[0].Name = proto.String(name)
		request.ProtoFile[0].Service[0].Method[0].InputType = proto.String(".echo.v1." + name)
		if response := Generate(request, Kotlin); response.GetError() != "" {
			t.Fatalf("%s: %s", name, response.GetError())
		}
	}
}
