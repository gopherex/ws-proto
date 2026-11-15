package mobilegen

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	echov1 "github.com/gopherex/ws-proto/example/proto/echo/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

func echoRequest() *pluginpb.CodeGeneratorRequest {
	f := protodesc.ToFileDescriptorProto(echov1.File_proto_echo_v1_echo_proto)
	return &pluginpb.CodeGeneratorRequest{FileToGenerate: []string{f.GetName()}, ProtoFile: []*descriptorpb.FileDescriptorProto{f}}
}

func TestGenerateEcho(t *testing.T) {
	t.Parallel()
	for _, language := range []Language{Swift, Kotlin} {
		t.Run(string(language), func(t *testing.T) {
			t.Parallel()
			response := Generate(echoRequest(), language)
			if response.GetError() != "" {
				t.Fatal(response.GetError())
			}
			if len(response.File) != 1 {
				t.Fatalf("got %d files", len(response.File))
			}
			content := response.File[0].GetContent()
			extension := ".swift"
			if language == Kotlin {
				extension = ".kt"
			}
			golden, err := os.ReadFile(filepath.Join("testdata", "echo"+extension))
			if err != nil {
				t.Fatal(err)
			}
			if content != string(golden) {
				t.Error("generated client differs from golden fixture")
			}
			for _, method := range []string{"Unary", "ServerStream", "ClientStream", "Bidi"} {
				if !strings.Contains(content, method) {
					t.Errorf("missing method %s", method)
				}
			}
			for _, required := range []string{"CallOptions", "transport"} {
				if !strings.Contains(content, required) {
					t.Errorf("missing %s", required)
				}
			}
			if response.GetSupportedFeatures()&uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL) == 0 {
				t.Error("proto3 optional not advertised")
			}
		})
	}
}

func TestGenerateRejectsInvalidInputs(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		edit func(*pluginpb.CodeGeneratorRequest)
	}{
		{"unknown option", func(r *pluginpb.CodeGeneratorRequest) { r.Parameter = proto.String("surprise=yes") }},
		{"missing file", func(r *pluginpb.CodeGeneratorRequest) { r.FileToGenerate = []string{"missing.proto"} }},
		{"duplicate file", func(r *pluginpb.CodeGeneratorRequest) {
			r.FileToGenerate = append(r.FileToGenerate, r.FileToGenerate[0])
		}},
		{"editions", func(r *pluginpb.CodeGeneratorRequest) { r.ProtoFile[0].Syntax = proto.String("editions") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := echoRequest()
			test.edit(r)
			for _, language := range []Language{Swift, Kotlin} {
				response := Generate(r, language)
				if response.GetError() == "" || len(response.File) != 0 {
					t.Fatalf("expected atomic failure for %s: %v", language, response)
				}
			}
		})
	}
}

func TestRunProtocol(t *testing.T) {
	t.Parallel()
	input, err := proto.Marshal(echoRequest())
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := Run(bytes.NewReader(input), &output, Swift); err != nil {
		t.Fatal(err)
	}
	var response pluginpb.CodeGeneratorResponse
	if err := proto.Unmarshal(output.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.GetError() != "" || len(response.File) != 1 {
		t.Fatal(&response)
	}
	if err := Run(strings.NewReader("\xff"), &output, Swift); err == nil {
		t.Fatal("invalid protobuf request accepted")
	}
}

func BenchmarkGenerate(b *testing.B) {
	for _, language := range []Language{Swift, Kotlin} {
		b.Run(string(language), func(b *testing.B) {
			request := echoRequest()
			for b.Loop() {
				Generate(request, language)
			}
		})
	}
}
