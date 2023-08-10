package document

import (
	"github.com/blues/jsonata-go"
	"github.com/vmihailenco/msgpack"
)

type Document struct {
	bin   []byte
	cache map[string]any
}

func NewDocumentFromJSON(str string) (*Document, error) {
	var json map[string]any
	err := msgpack.Unmarshal([]byte(str), &json)
	if err != nil {
		return nil, err
	}

	return NewDocumentFromMap(json)
}

func NewDocumentFromMap(json map[string]any) (*Document, error) {
	res, err := msgpack.Marshal(json)
	if err != nil {
		return nil, err
	}

	return &Document{
		bin:   res,
		cache: json,
	}, nil
}

func (d *Document) Eval(key string) (any, error) {
	if d.cache == nil {
		err := msgpack.Unmarshal(d.bin, &d.cache)
		if err != nil {
			return nil, err
		}
	}

	e := jsonata.MustCompile(key)
	return e.Eval(d.cache)
}
