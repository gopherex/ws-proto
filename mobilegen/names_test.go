package mobilegen

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

func TestMessageNames(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		file          *descriptorpb.FileDescriptorProto
		swift, kotlin string
	}{
		{"default", namingFile("api.v1", nil), "Api_V1_Outer.TypeMessage", "`api`.`v1`.`Names`.`Outer`.`Type`"},
		{"explicit", namingFile("api.v1", &descriptorpb.FileOptions{SwiftPrefix: proto.String("RPC"), JavaPackage: proto.String("com.example"), JavaOuterClassname: proto.String("Messages")}), "RPCOuter.TypeMessage", "`com`.`example`.`Messages`.`Outer`.`Type`"},
		{"multiple", namingFile("api.v1", &descriptorpb.FileOptions{JavaMultipleFiles: proto.Bool(true)}), "Api_V1_Outer.TypeMessage", "`api`.`v1`.`Outer`.`Type`"},
		{"empty prefix", namingFile("api.v1", &descriptorpb.FileOptions{SwiftPrefix: proto.String("")}), "Outer.TypeMessage", "`api`.`v1`.`Names`.`Outer`.`Type`"},
		{"no package", namingFile("", nil), "Outer.TypeMessage", "`Names`.`Outer`.`Type`"},
		{"package underscores", namingFile("foo_bar.v1", nil), "FooBar_V1_Outer.TypeMessage", "`foo_bar`.`v1`.`Names`.`Outer`.`Type`"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			file, err := protodesc.NewFile(test.file, nil)
			if err != nil {
				t.Fatal(err)
			}
			message := file.Messages().Get(0).Messages().Get(0)
			if got := swiftName(message, file, settings{}); got != test.swift {
				t.Errorf("Swift: got %q, want %q", got, test.swift)
			}
			if got := kotlinName(message); got != test.kotlin {
				t.Errorf("Kotlin: got %q, want %q", got, test.kotlin)
			}
		})
	}
}

func namingFile(pkg string, options *descriptorpb.FileOptions) *descriptorpb.FileDescriptorProto {
	return &descriptorpb.FileDescriptorProto{Name: proto.String("names.proto"), Package: proto.String(pkg), Syntax: proto.String("proto3"), Options: options,
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Outer"), NestedType: []*descriptorpb.DescriptorProto{{Name: proto.String("Type")}}}},
	}
}

func TestSwiftSanitization(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{"String": "StringMessage", "StringMessage": "StringMessageMessage", "StringMessageMessage": "StringMessageMessageMessage", "Type": "TypeMessage", "___": "___Message", "normal_name": "normal_name", "MyMessage": "MyMessage", "class": "classMessage"} {
		if got := swiftMessageName(input); got != want {
			t.Errorf("%s: got %s, want %s", input, got, want)
		}
	}
}

func TestJavaOuterCollision(t *testing.T) {
	t.Parallel()
	file := namingFile("test", nil)
	file.MessageType[0].NestedType = append(file.MessageType[0].NestedType, &descriptorpb.DescriptorProto{Name: proto.String("Names")})
	descriptor, err := protodesc.NewFile(file, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := javaOuter(descriptor); got != "NamesOuterClass" {
		t.Fatal(got)
	}
}

func TestImportedTypes(t *testing.T) {
	t.Parallel()
	request := echoRequest()
	dependency := namingFile("other.v1", &descriptorpb.FileOptions{JavaMultipleFiles: proto.Bool(true), JavaPackage: proto.String("com.other"), SwiftPrefix: proto.String("Other")})
	request.ProtoFile = append([]*descriptorpb.FileDescriptorProto{dependency}, request.ProtoFile...)
	file := request.ProtoFile[1]
	file.Dependency = []string{dependency.GetName()}
	file.Service[0].Method[0].InputType = proto.String(".other.v1.Outer.Type")
	request.Parameter = proto.String("Mnames.proto=OtherMessages")
	response := Generate(request, Swift)
	if response.GetError() != "" {
		t.Fatal(response.GetError())
	}
	content := response.File[0].GetContent()
	if !strings.Contains(content, "import OtherMessages") || !strings.Contains(content, "OtherMessages.OtherOuter.TypeMessage") {
		t.Fatal(content)
	}
	request.Parameter = nil
	response = Generate(request, Kotlin)
	if response.GetError() != "" {
		t.Fatal(response.GetError())
	}
	if !strings.Contains(response.File[0].GetContent(), "`com`.`other`.`Outer`.`Type`") {
		t.Fatal(response.File[0].GetContent())
	}
}

func TestMethodNameCollision(t *testing.T) {
	t.Parallel()
	request := echoRequest()
	method := proto.Clone(request.ProtoFile[0].Service[0].Method[0])
	cloned := &descriptorpb.MethodDescriptorProto{}
	proto.Merge(cloned, method)
	cloned.Name = proto.String("unary")
	request.ProtoFile[0].Service[0].Method = append(request.ProtoFile[0].Service[0].Method, cloned)
	for _, language := range []Language{Swift, Kotlin} {
		if response := Generate(request, language); !strings.Contains(response.GetError(), "collide") {
			t.Fatal(response)
		}
	}
}

func TestServiceLessFile(t *testing.T) {
	t.Parallel()
	request := &pluginpb.CodeGeneratorRequest{FileToGenerate: []string{"names.proto"}, ProtoFile: []*descriptorpb.FileDescriptorProto{namingFile("test", nil)}}
	for _, language := range []Language{Swift, Kotlin} {
		if response := Generate(request, language); response.GetError() != "" || len(response.File) != 0 {
			t.Fatal(response)
		}
	}
}

func TestClientNameCollision(t *testing.T) {
	t.Parallel()
	for _, language := range []Language{Swift, Kotlin} {
		t.Run(string(language), func(t *testing.T) {
			t.Parallel()
			request := echoRequest()
			file := request.ProtoFile[0]
			file.Options.JavaMultipleFiles = proto.Bool(true)
			file.MessageType = append(file.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("EchoServiceClient")})
			if response := Generate(request, language); !strings.Contains(response.GetError(), "collision") || len(response.File) != 0 {
				t.Fatal(response)
			}
		})
	}
}

func TestOptionsAndInvalidNames(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		language Language
		edit     func(*pluginpb.CodeGeneratorRequest)
	}{
		{"invalid prefix", Swift, func(r *pluginpb.CodeGeneratorRequest) {
			r.ProtoFile[0].Options.SwiftPrefix = proto.String("invalid-prefix")
		}},
		{"invalid module", Swift, func(r *pluginpb.CodeGeneratorRequest) { r.Parameter = proto.String("Mfoo.proto=invalid.module") }},
		{"invalid package", Kotlin, func(r *pluginpb.CodeGeneratorRequest) { r.ProtoFile[0].Options.JavaPackage = proto.String("../../bad") }},
		{"invalid outer class", Kotlin, func(r *pluginpb.CodeGeneratorRequest) {
			r.ProtoFile[0].Options.JavaOuterClassname = proto.String("class")
		}},
		{"invalid Java message", Kotlin, func(r *pluginpb.CodeGeneratorRequest) {
			r.ProtoFile[0].MessageType[0].Name = proto.String("class")
			r.ProtoFile[0].Service[0].Method[0].InputType = proto.String(".echo.v1.class")
		}},
		{"unsafe path", Swift, func(r *pluginpb.CodeGeneratorRequest) {
			r.ProtoFile[0].Name = proto.String("../echo.proto")
			r.FileToGenerate = []string{"../echo.proto"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := echoRequest()
			test.edit(request)
			if response := Generate(request, test.language); response.GetError() == "" || len(response.File) != 0 {
				t.Fatal(response)
			}
		})
	}
	request := echoRequest()
	request.Parameter = proto.String("Visibility=Internal")
	response := Generate(request, Swift)
	if response.GetError() != "" || !strings.Contains(response.File[0].GetContent(), "internal final class") {
		t.Fatal(response)
	}
}

func TestMethodNames(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{"Class": "class", "Init": "initRPC", "Deinit": "deinitRPC", "Self": "selfRPC", "GetURL": "getURL", "Upload_Files": "upload_Files", "": ""} {
		if got := methodName(input); got != want {
			t.Errorf("%s: got %s, want %s", input, got, want)
		}
	}
}

func BenchmarkMessageNames(b *testing.B) {
	descriptor, err := protodesc.NewFile(namingFile("test", nil), nil)
	if err != nil {
		b.Fatal(err)
	}
	message := descriptor.Messages().Get(0).Messages().Get(0)
	for _, test := range []struct {
		name string
		run  func(protoreflect.MessageDescriptor) string
	}{
		{"kotlin", kotlinName}, {"swift", func(m protoreflect.MessageDescriptor) string { return swiftName(m, descriptor, settings{}) }},
	} {
		b.Run(test.name, func(b *testing.B) {
			for b.Loop() {
				test.run(message)
			}
		})
	}
}
