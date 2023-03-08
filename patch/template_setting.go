package patch

import (
	"fmt"
	"reflect"

	json "github.com/json-iterator/go"

	olivere "github.com/olivere/elastic/v7"
)

type IndicesGetComponentTemplate struct {
	olivere.IndicesGetComponentTemplate
}

type IndicesGetIndexTemplate struct {
	olivere.IndicesGetIndexTemplate
}

func (o *IndicesGetComponentTemplate) UnmarshalJSON(data []byte) error {

	tmp := &olivere.IndicesGetComponentTemplate{}
	if err := json.ConfigCompatibleWithStandardLibrary.Unmarshal(data, &tmp); err != nil {
		return err
	}

	o.IndicesGetComponentTemplate = *tmp

	if o.Template != nil && o.Template.Settings != nil {
		walk(reflect.Value{}, reflect.Value{}, o.Template.Settings)
	}

	return nil
}

func (o *IndicesGetIndexTemplate) UnmarshalJSON(data []byte) error {

	tmp := &olivere.IndicesGetIndexTemplate{}
	if err := json.ConfigCompatibleWithStandardLibrary.Unmarshal(data, &tmp); err != nil {
		return err
	}

	o.IndicesGetIndexTemplate = *tmp

	if o.Template != nil && o.Template.Settings != nil {
		walk(reflect.Value{}, reflect.Value{}, o.Template.Settings)
	}

	return nil
}

func walk(m reflect.Value, key reflect.Value, v any) {
	switch v := v.(type) {
	case []interface{}:
		for i, c := range v {
			walk(reflect.ValueOf(v), reflect.ValueOf(i), c)
		}
	case map[string]interface{}:
		for k, c := range v {
			walk(reflect.ValueOf(v), reflect.ValueOf(k), c)
		}
	default:
		rv := reflect.ValueOf(v)
		switch m.Kind() {
		case reflect.Map:
			if rv.Kind() == reflect.Float64 {
				str := fmt.Sprintf("%d", int64(v.(float64)))
				m.SetMapIndex(key, reflect.ValueOf(str))
			}
		case reflect.Slice:
			if rv.Kind() == reflect.Float64 {
				str := fmt.Sprintf("%d", int64(v.(float64)))
				m.Index(int(key.Int())).Set(reflect.ValueOf(str))
			}
		}
	}
}

// ConvertComponentTemplateSetting permit to convert all number to string on component template settings
func ConvertComponentTemplateSetting(actualByte []byte, expectedByte []byte) ([]byte, []byte, error) {
	actual := &IndicesGetComponentTemplate{}
	expected := &IndicesGetComponentTemplate{}
	var err error

	if err = json.ConfigCompatibleWithStandardLibrary.Unmarshal(actualByte, actual); err != nil {
		return nil, nil, err
	}

	if err = json.ConfigCompatibleWithStandardLibrary.Unmarshal(expectedByte, expected); err != nil {
		return nil, nil, err
	}

	actualByte, err = json.ConfigCompatibleWithStandardLibrary.Marshal(actual)
	if err != nil {
		return nil, nil, err
	}

	expectedByte, err = json.ConfigCompatibleWithStandardLibrary.Marshal(expected)
	if err != nil {
		return nil, nil, err
	}

	return actualByte, expectedByte, nil
}

// ConvertIndexTemplateSetting  permit to convert all number to string on component template settings
func ConvertIndexTemplateSetting(actualByte []byte, expectedByte []byte) ([]byte, []byte, error) {
	actual := &IndicesGetIndexTemplate{}
	expected := &IndicesGetIndexTemplate{}
	var err error

	if err = json.ConfigCompatibleWithStandardLibrary.Unmarshal(actualByte, actual); err != nil {
		return nil, nil, err
	}

	if err = json.ConfigCompatibleWithStandardLibrary.Unmarshal(expectedByte, expected); err != nil {
		return nil, nil, err
	}

	actualByte, err = json.ConfigCompatibleWithStandardLibrary.Marshal(actual)
	if err != nil {
		return nil, nil, err
	}

	expectedByte, err = json.ConfigCompatibleWithStandardLibrary.Marshal(expected)
	if err != nil {
		return nil, nil, err
	}

	return actualByte, expectedByte, nil
}
