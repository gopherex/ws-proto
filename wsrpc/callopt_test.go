package wsrpc

import (
	"reflect"
	"testing"
)

func TestCallHeaders(t *testing.T) {
	got := CallHeaders(WithCallHeader("a", "1"), WithCallHeaders(map[string]string{"b": "2"}))
	want := map[string]string{"a": "1", "b": "2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CallHeaders = %v, want %v", got, want)
	}
}

func TestCallHeadersEmpty(t *testing.T) {
	if got := CallHeaders(); got != nil {
		t.Fatalf("CallHeaders() = %v, want nil", got)
	}
}
