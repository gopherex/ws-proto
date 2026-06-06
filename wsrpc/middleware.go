package wsrpc

// Middleware wraps a Handler to add cross-cutting behavior (auth, logging,
// recovery, metrics, rate limiting). It runs once per RPC stream, before the
// handler. A middleware can inspect the Stream (Method, Header, deadline via
// Context), short-circuit by returning an error without calling next, or wrap
// the call to observe its result. It is uniform across all four RPC kinds.
type Middleware func(next Handler) Handler

// chain composes middleware so the first listed runs outermost:
// chain(h, a, b, c) yields a(b(c(h))) — a enters first and exits last.
func chain(h Handler, mw []Middleware) Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}
