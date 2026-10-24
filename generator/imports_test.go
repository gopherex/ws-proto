package generator

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

// A service of methods of one kind only must not import what only the
// other kinds use: Go rejects an unused import.
func TestImportsUsedPerKind(t *testing.T) {
	kinds := map[string][2]bool{ // client streams, server streams
		"unary":         {false, false},
		"server_stream": {false, true},
		"client_stream": {true, false},
		"bidi":          {true, true},
	}
	for name, kind := range kinds {
		t.Run(name, func(t *testing.T) {
			src := generate(t, kind[0], kind[1])
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "x_ws.pb.go", src, 0)
			if err != nil {
				t.Fatalf("parse: %v\n%s", err, src)
			}
			used := map[string]bool{}
			ast.Inspect(f, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok {
					if id, ok := sel.X.(*ast.Ident); ok {
						used[id.Name] = true
					}
				}
				return true
			})
			for _, imp := range f.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				local := path[strings.LastIndex(path, "/")+1:]
				if imp.Name != nil {
					local = imp.Name.Name
				}
				if local != "_" && !used[local] {
					t.Errorf("%q imported and not used\n%s", path, src)
				}
			}
		})
	}
}

// generate runs the generator on a file with one service of one method.
func generate(t *testing.T, clientStreams, serverStreams bool) string {
	t.Helper()
	file := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("kind/v1/kind.proto"),
		Package: proto.String("kind.v1"),
		Syntax:  proto.String("proto3"),
		Options: &descriptorpb.FileOptions{GoPackage: proto.String("example.com/kind/v1;kindv1")},
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Request")}, {Name: proto.String("Response")},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: proto.String("KindService"),
			Method: []*descriptorpb.MethodDescriptorProto{{
				Name:            proto.String("Call"),
				InputType:       proto.String(".kind.v1.Request"),
				OutputType:      proto.String(".kind.v1.Response"),
				ClientStreaming: proto.Bool(clientStreams),
				ServerStreaming: proto.Bool(serverStreams),
			}},
		}},
	}
	plugin, err := protogen.Options{}.New(&pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{file.GetName()},
		ProtoFile:      []*descriptorpb.FileDescriptorProto{file},
	})
	if err != nil {
		t.Fatal(err)
	}
	g, _ := NewGenerator(plugin, &PluginSettings{})
	if err := g.Generate(); err != nil {
		t.Fatal(err)
	}
	res := plugin.Response()
	if res.Error != nil {
		t.Fatal(res.GetError())
	}
	if len(res.GetFile()) != 1 {
		t.Fatalf("generated %d files", len(res.GetFile()))
	}
	return res.GetFile()[0].GetContent()
}
