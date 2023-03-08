package patch

import (
	"encoding/json"
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
)

func TestConvertTemplateSetting(t *testing.T) {

	actual := &IndicesGetComponentTemplate{
		IndicesGetComponentTemplate: elastic.IndicesGetComponentTemplate{
			Template: &elastic.IndicesGetComponentTemplateData{
				Settings: map[string]any{
					"test": "plop",
					"property": map[string]any{
						"plop": 100,
					},
					"list": []any{
						200,
						300,
					},
				},
			},
		},
	}

	expected := &IndicesGetComponentTemplate{
		IndicesGetComponentTemplate: elastic.IndicesGetComponentTemplate{
			Template: &elastic.IndicesGetComponentTemplateData{
				Settings: map[string]any{
					"test": "plop",
					"property": map[string]any{
						"plop": "100",
					},
					"list": []any{
						"200",
						"300",
					},
				},
			},
		},
	}

	acualByte, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}

	expectedByte, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}

	acualByte, expectedByte, err = ConvertTemplateSetting(acualByte, expectedByte)
	assert.NoError(t, err)
	assert.Equal(t, expectedByte, acualByte)

}
