package app

import "net/http"

const (
	Get HTTPMethod = iota + 1
	Post
	Head
	Put
	Patch
	Delete
	Options
)

type Headers struct {
	setHeaderEntryMap   map[string]string
	addHeaderEntrySlice []AddHeaderEntry
}

func (x *Headers) Set(name, value string) {
	if x.setHeaderEntryMap == nil {
		x.setHeaderEntryMap = make(map[string]string)
	}

	x.setHeaderEntryMap[name] = value
}

func (x *Headers) Add(name, value string) {
	x.addHeaderEntrySlice = append(
		x.addHeaderEntrySlice,
		AddHeaderEntry{
			Name:  name,
			Value: value,
		},
	)
}

type AddHeaderEntry struct {
	Name  string
	Value string
}

type (
	HTTPResponse struct {
		Headers Headers
		Data    []byte
		Code    int
	}

	HTTPMethod int
	HandlerFn  func(request *http.Request) (*HTTPResponse, error)
)

func (x *Headers) GetSetEntryMap() map[string]string {
	return x.setHeaderEntryMap
}

func (x *Headers) GetAddEntrySlice() []AddHeaderEntry {
	return x.addHeaderEntrySlice
}
