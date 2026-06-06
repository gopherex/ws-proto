package generator

import "flag"

// PluginSettings holds plugin parameters passed through --go-ws_opt. No options
// are defined yet; the struct and RegisterFlags exist as the wiring point for
// future flags so main.go and NewGenerator stay stable when one is added.
type PluginSettings struct{}

// RegisterFlags binds plugin settings to a flag set. Wire the flag set's Set
// method as protogen.Options.ParamFunc so --go-ws_opt=key=value populate these.
// No flags are registered yet.
func (s *PluginSettings) RegisterFlags(fs *flag.FlagSet) {}
