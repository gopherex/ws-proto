package wsrpc

type callConfig struct{ headers map[string]string }

// CallOption configures a single generated client RPC.
type CallOption func(*callConfig)

// WithCallHeader sets one request metadata header sent on the opening frame.
func WithCallHeader(key, value string) CallOption {
	return func(c *callConfig) {
		if c.headers == nil {
			c.headers = map[string]string{}
		}
		c.headers[key] = value
	}
}

// WithCallHeaders sets several request metadata headers at once.
func WithCallHeaders(h map[string]string) CallOption {
	return func(c *callConfig) {
		if c.headers == nil {
			c.headers = map[string]string{}
		}
		for k, v := range h {
			c.headers[k] = v
		}
	}
}

// CallHeaders resolves options into a headers map (used by generated code). Returns nil if empty.
func CallHeaders(opts ...CallOption) map[string]string {
	var c callConfig
	for _, o := range opts {
		o(&c)
	}
	return c.headers
}
