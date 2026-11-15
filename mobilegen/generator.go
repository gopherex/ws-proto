// Package mobilegen generates native Swift and Kotlin clients for ws-proto.
// Message serialization stays with SwiftProtobuf and Square Wire.
package mobilegen

import (
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

// Language identifies the output runtime.
type Language string

const (
	Swift  Language = "swift"
	Kotlin Language = "kotlin"
)

type settings struct {
	modules    map[string]string
	visibility string
}

func parseSettings(parameter string, language Language) (settings, error) {
	result := settings{modules: make(map[string]string), visibility: "public"}
	if parameter == "" {
		return result, nil
	}
	for _, option := range strings.Split(parameter, ",") {
		key, value, ok := strings.Cut(option, "=")
		if language == Swift && ok && strings.HasPrefix(key, "M") && strings.HasSuffix(key, ".proto") && validIdentifier(value) {
			result.modules[strings.TrimPrefix(key, "M")] = value
			continue
		}
		if language == Swift && key == "Visibility" && (value == "Public" || value == "Internal") {
			result.visibility = strings.ToLower(value)
			continue
		}
		return result, fmt.Errorf("unsupported %s generator option %q", language, option)
	}
	return result, nil
}

// Generate produces a protoc response. Errors are reported in the response with
// no partial files, as required by the protoc plugin protocol.
func Generate(request *pluginpb.CodeGeneratorRequest, language Language) *pluginpb.CodeGeneratorResponse {
	response := &pluginpb.CodeGeneratorResponse{SupportedFeatures: proto.Uint64(uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL))}
	files, err := generate(request, language)
	if err != nil {
		response.Error = proto.String(err.Error())
		return response
	}
	response.File = files
	return response
}

func generate(request *pluginpb.CodeGeneratorRequest, language Language) ([]*pluginpb.CodeGeneratorResponse_File, error) {
	if language != Swift && language != Kotlin {
		return nil, fmt.Errorf("unsupported language %q", language)
	}
	options, err := parseSettings(request.GetParameter(), language)
	if err != nil {
		return nil, err
	}
	for _, file := range request.GetProtoFile() {
		if file.GetSyntax() == "editions" {
			return nil, fmt.Errorf("%s: editions are not supported; use proto2 or proto3", file.GetName())
		}
	}
	registry, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{File: request.GetProtoFile()})
	if err != nil {
		return nil, fmt.Errorf("resolve protobuf descriptors: %w", err)
	}
	var names kotlinNames
	if language == Kotlin {
		names, err = resolveKotlinNames(registry)
		if err != nil {
			return nil, err
		}
	}
	var generated []*pluginpb.CodeGeneratorResponse_File
	seen := make(map[string]bool)
	symbols := make(map[string]string)
	registry.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		for i := 0; i < file.Messages().Len(); i++ {
			message := file.Messages().Get(i)
			key := ""
			if language == Swift {
				key = options.modules[file.Path()] + "." + swiftMessageName(swiftPrefix(file)+string(message.Name()))
			} else {
				key = names[file.Path()] + "." + string(message.Name())
			}
			if key != "" {
				symbols[key] = string(message.FullName())
			}
		}
		for i := 0; i < file.Enums().Len(); i++ {
			enum := file.Enums().Get(i)
			if language == Swift {
				symbols[options.modules[file.Path()]+"."+swiftPrefix(file)+string(enum.Name())] = string(enum.FullName())
			} else {
				symbols[names[file.Path()]+"."+string(enum.Name())] = string(enum.FullName())
			}
		}
		return true
	})
	for _, name := range request.GetFileToGenerate() {
		if seen[name] {
			return nil, fmt.Errorf("duplicate input file %q", name)
		}
		seen[name] = true
		file, err := registry.FindFileByPath(name)
		if err != nil {
			return nil, fmt.Errorf("find input %q: %w", name, err)
		}
		if path.IsAbs(name) || path.Clean(name) != name || strings.HasPrefix(name, "../") || strings.Contains(name, "\\") {
			return nil, fmt.Errorf("unsafe proto path %q", name)
		}
		if file.Services().Len() == 0 {
			continue
		}
		for i := 0; i < file.Services().Len(); i++ {
			service := file.Services().Get(i)
			if language == Kotlin {
				if err := validateWireService(service); err != nil {
					return nil, err
				}
			}
			if err := validateService(service); err != nil {
				return nil, err
			}
			key := names[file.Path()] + "." + string(service.Name()) + "Client"
			if language == Swift {
				key = options.modules[file.Path()] + "." + swiftPrefix(file) + string(service.Name()) + "Client"
			}
			if prior, exists := symbols[key]; exists {
				return nil, fmt.Errorf("client name collision %q between %s and %s", key, prior, service.FullName())
			}
			symbols[key] = string(service.FullName())
		}
		if language == Swift {
			content, err := generateSwift(file, options)
			if err != nil {
				return nil, err
			}
			generated = append(generated, output(strings.TrimSuffix(name, path.Ext(name))+"_ws.pb.swift", content))
		} else {
			for i := 0; i < file.Services().Len(); i++ {
				service := file.Services().Get(i)
				filename := path.Join(strings.ReplaceAll(names[file.Path()], ".", "/"), string(service.Name())+"Client.kt")
				generated = append(generated, output(filename, generateKotlin(service, names)))
			}
		}
	}
	return generated, nil
}

func output(name, content string) *pluginpb.CodeGeneratorResponse_File {
	return &pluginpb.CodeGeneratorResponse_File{Name: proto.String(name), Content: proto.String(content)}
}

func validateService(service protoreflect.ServiceDescriptor) error {
	names := make(map[string]bool)
	for i := 0; i < service.Methods().Len(); i++ {
		method := service.Methods().Get(i)
		name := methodName(string(method.Name()))
		if names[name] {
			return fmt.Errorf("%s: methods collide after lowercasing their first letter: %s", service.FullName(), name)
		}
		names[name] = true
	}
	return nil
}

func fileOptions(file protoreflect.FileDescriptor) *descriptorpb.FileOptions {
	switch options := file.Options().(type) {
	case *descriptorpb.FileOptions:
		if options != nil {
			return options
		}
		return &descriptorpb.FileOptions{}
	default:
		return &descriptorpb.FileOptions{}
	}
}

func sortedKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Run serves one protoc request on stdin/stdout without requiring Go-specific
// options such as go_package on the input schemas.
func Run(input io.Reader, output io.Writer, language Language) error {
	data, err := io.ReadAll(input)
	if err != nil {
		return fmt.Errorf("read protoc request: %w", err)
	}
	var request pluginpb.CodeGeneratorRequest
	if err := proto.Unmarshal(data, &request); err != nil {
		return fmt.Errorf("decode protoc request: %w", err)
	}
	encoded, err := proto.Marshal(Generate(&request, language))
	if err != nil {
		return fmt.Errorf("encode protoc response: %w", err)
	}
	if _, err := output.Write(encoded); err != nil {
		return fmt.Errorf("write protoc response: %w", err)
	}
	return nil
}
