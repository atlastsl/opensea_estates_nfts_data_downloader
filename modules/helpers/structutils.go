package helpers

import "github.com/mitchellh/mapstructure"

func ConvertMapToStruct(m map[string]any, target interface{}) error {
	config := &mapstructure.DecoderConfig{
		ErrorUnused: true,
		Result:      target,
	}
	decoder, e1 := mapstructure.NewDecoder(config)
	if e1 != nil {
		return e1
	}
	e2 := decoder.Decode(m)
	if e2 != nil {
		return e2
	}
	return nil
}
