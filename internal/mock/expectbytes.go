package mock

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type BytesExpectation interface {
	AsJson(v any)
	AsString(v string)
	Empty()
	Value(v []byte)
}

type expectbytes struct {
	value []byte
}

func (e *expectbytes) AsJson(v any) {
	var err error
	if e.value, err = json.Marshal(v); err != nil {
		panic(fmt.Sprintf("error marshalling json: %s", err))
	}
}

func (e *expectbytes) AsString(v string) {
	e.value = []byte(v)
}

func (e *expectbytes) Empty() {
	e.value = []byte{}
}

func (e *expectbytes) Equal(v []byte) bool {
	return e == nil || // no expectation set -> always true
		(e.value == nil && v != nil) || // expected value is nil -> any non-nil value is ok
		(e.value != nil && bytes.Equal(e.value, v)) // expected value is non-nil -> must match
}

func (e *expectbytes) String() string {
	if e == nil {
		return "<any>"
	}
	return fmt.Sprintf("%q", string(e.value))
}

func (e *expectbytes) Value(v []byte) {
	e.value = v
}
