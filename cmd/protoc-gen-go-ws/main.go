package main

import (
	"flag"

	"github.com/gopherex/ws-proto/generator"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/types/pluginpb"
)

func main() {
	var flags flag.FlagSet
	settings := &generator.PluginSettings{}
	settings.RegisterFlags(&flags)

	protogen.Options{ParamFunc: flags.Set}.Run(func(plugin *protogen.Plugin) error {
		plugin.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
		g, err := generator.NewGenerator(plugin, settings)
		if err != nil {
			return err
		}
		return g.Generate()
	})
}
