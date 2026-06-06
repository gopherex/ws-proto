package generator

import "flag"

// PluginSettings holds plugin parameters passed through --go-ws_opt.
type PluginSettings struct {
	// RegisterFuncName lets a caller override the generated registrar prefix;
	// empty uses the default "Register<Service>Handler". Reserved for future
	// use; the default is currently always applied.
	RegisterFuncName string
}

// RegisterFlags binds plugin settings to a flag set. Wire the flag set's Set
// method as protogen.Options.ParamFunc so --go-ws_opt=key=value populate these.
func (s *PluginSettings) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&s.RegisterFuncName, "register_func_name", "", "override registrar function name prefix (reserved)")
}
