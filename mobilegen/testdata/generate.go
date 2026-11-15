// Generates the cross-language Echo client from the checked-in Go descriptor.
// Run from the repository root; no protoc executable is needed for RPC clients.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	echov1 "github.com/gopherex/ws-proto/example/proto/echo/v1"
	"github.com/gopherex/ws-proto/mobilegen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

func main() {
	if err := generate(); err != nil {
		log.Fatal(err)
	}
}

func generate() error {
	language := flag.String("language", "swift", "swift or kotlin")
	directory := flag.String("out", "", "generated client output directory")
	parameter := flag.String("parameter", "", "protoc plugin parameters")
	descriptorPath := flag.String("descriptor-out", "", "optional output descriptor set for the official message generator")
	flag.Parse()
	if *directory == "" {
		return fmt.Errorf("-out is required")
	}
	file := protodesc.ToFileDescriptorProto(echov1.File_proto_echo_v1_echo_proto)
	response := mobilegen.Generate(&pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{file.GetName()},
		ProtoFile:      []*descriptorpb.FileDescriptorProto{file},
		Parameter:      parameter,
	}, mobilegen.Language(*language))
	if response.GetError() != "" {
		return fmt.Errorf("generate: %s", response.GetError())
	}
	for _, file := range response.GetFile() {
		filename := filepath.Join(*directory, filepath.FromSlash(file.GetName()))
		if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
			return fmt.Errorf("create output directory: %w", err)
		}
		if err := os.WriteFile(filename, []byte(file.GetContent()), 0o644); err != nil {
			return fmt.Errorf("write generated client: %w", err)
		}
	}
	if *descriptorPath != "" {
		encoded, err := proto.Marshal(&descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{file}})
		if err != nil {
			return fmt.Errorf("encode descriptor: %w", err)
		}
		if err := os.WriteFile(*descriptorPath, encoded, 0o644); err != nil {
			return fmt.Errorf("write descriptor: %w", err)
		}
	}
	return nil
}
