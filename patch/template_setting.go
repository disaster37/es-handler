package patch

import (
	"fmt"

	json "github.com/json-iterator/go"
)

type ComponentTemplate struct {
	Template *ComponentTemplateData `json:"template"`
	Version  *int64                 `json:"version,omitempty"`
	Meta     map[string]any         `json:"_meta,omitempty"`
}

type ComponentTemplateData struct {
	Settings map[string]any `json:"settings,omitempty"`
	Mappings map[string]any `json:"mappings,omitempty"`
	Aliases  map[string]any `json:"aliases,omitempty"`
}

type ComponentTemplateGetResponse struct {
	ComponentTemplates []struct {
		Name              string             `json:"name"`
		ComponentTemplate *ComponentTemplate `json:"component_template"`
	} `json:"component_templates"`
}

type IndexTemplate struct {
	IndexPatterns []string           `json:"index_patterns"`
	Template      *IndexTemplateData `json:"template"`
	Priority      int                `json:"priority,omitempty"`
	Version       int                `json:"version,omitempty"`
	ComposedOf    []string           `json:"composed_of,omitempty"`
	DataStream    map[string]any     `json:"data_stream,omitempty"`
	Meta          map[string]any     `json:"_meta,omitempty"`
}

type IndexTemplateData struct {
	Settings map[string]any `json:"settings,omitempty"`
	Mappings map[string]any `json:"mappings,omitempty"`
	Aliases  map[string]any `json:"aliases,omitempty"`
}

type IndexTemplateGetResponse struct {
	IndexTemplates []struct {
		Name          string         `json:"name"`
		IndexTemplate *IndexTemplate `json:"index_template"`
	} `json:"index_templates"`
}

// walk converts every float64 in nested maps/slices to a decimal string.
func walk(v any) any {
	switch v := v.(type) {
	case []any:
		for i, c := range v {
			v[i] = walk(c)
		}
		return v
	case map[string]any:
		for k, c := range v {
			v[k] = walk(c)
		}
		return v
	case float64:
		return fmt.Sprintf("%d", int64(v))
	default:
		return v
	}
}

// walkedSettings converts all numeric values in a settings map to strings,
// returning nil when settings is nil.
func walkedSettings(settings map[string]any) map[string]any {
	if settings == nil {
		return nil
	}
	return walk(settings).(map[string]any)
}

// marshalTemplatePair marshals both templates back to JSON.
func marshalTemplatePair(actual, expected any) ([]byte, []byte, error) {
	actualByte, err := json.ConfigCompatibleWithStandardLibrary.Marshal(actual)
	if err != nil {
		return nil, nil, err
	}
	expectedByte, err := json.ConfigCompatibleWithStandardLibrary.Marshal(expected)
	if err != nil {
		return nil, nil, err
	}
	return actualByte, expectedByte, nil
}

// ConvertComponentTemplateSetting permit to convert all number to string on component template settings
func ConvertComponentTemplateSetting(actualByte, expectedByte []byte) ([]byte, []byte, error) {
	actual := &ComponentTemplate{}
	expected := &ComponentTemplate{}
	if err := json.ConfigCompatibleWithStandardLibrary.Unmarshal(actualByte, actual); err != nil {
		return nil, nil, err
	}
	if err := json.ConfigCompatibleWithStandardLibrary.Unmarshal(expectedByte, expected); err != nil {
		return nil, nil, err
	}
	if actual.Template != nil {
		actual.Template.Settings = walkedSettings(actual.Template.Settings)
	}
	if expected.Template != nil {
		expected.Template.Settings = walkedSettings(expected.Template.Settings)
	}
	return marshalTemplatePair(actual, expected)
}

// ConvertIndexTemplateSetting permit to convert all number to string on index template settings
func ConvertIndexTemplateSetting(actualByte, expectedByte []byte) ([]byte, []byte, error) {
	actual := &IndexTemplate{}
	expected := &IndexTemplate{}
	if err := json.ConfigCompatibleWithStandardLibrary.Unmarshal(actualByte, actual); err != nil {
		return nil, nil, err
	}
	if err := json.ConfigCompatibleWithStandardLibrary.Unmarshal(expectedByte, expected); err != nil {
		return nil, nil, err
	}
	if actual.Template != nil {
		actual.Template.Settings = walkedSettings(actual.Template.Settings)
	}
	if expected.Template != nil {
		expected.Template.Settings = walkedSettings(expected.Template.Settings)
	}
	return marshalTemplatePair(actual, expected)
}
