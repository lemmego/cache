package cache

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

// Codec turns values into the bytes a Store holds, and back.
//
// The driver never sees a Go value, which is what keeps a driver from needing
// to know about gob type registration, and what makes a value written by one
// process readable by another written in a different language.
type Codec interface {
	// Name identifies the codec in configuration and in error messages.
	Name() string
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONCodec is the default. It is readable in redis-cli, portable across
// languages, and cannot be tripped up by an unregistered type.
//
// Its limitation is Go type fidelity: a value round-tripped through any
// arrives as float64 rather than int, and unexported fields are dropped. Decode
// into a concrete type — which the generic API does — and neither matters.
type JSONCodec struct{}

func (JSONCodec) Name() string                       { return "json" }
func (JSONCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (JSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// GobCodec preserves Go types exactly, including unexported fields via
// GobEncoder. In exchange, any type reached through an interface field must be
// registered with gob.Register first, and the bytes are readable only by Go.
type GobCodec struct{}

func (GobCodec) Name() string { return "gob" }

func (GobCodec) Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (GobCodec) Unmarshal(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

// codecFor resolves a configured codec name.
func codecFor(name string) (Codec, bool) {
	switch name {
	case "", "json":
		return JSONCodec{}, true
	case "gob":
		return GobCodec{}, true
	default:
		return nil, false
	}
}
