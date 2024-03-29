package eshandler

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/jarcoal/httpmock"
	olivere "github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
)

var urlComponentTemplate = fmt.Sprintf("%s/_component_template/test", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestComponentTemplateGet() {

	result := &olivere.IndicesGetComponentTemplateResponse{}
	component := &olivere.IndicesGetComponentTemplate{
		Template: &olivere.IndicesGetComponentTemplateData{
			Settings: map[string]any{
				"index.refresh_interval": "5s",
			},
			Mappings: map[string]any{
				"_source.enabled":           true,
				"properties.host_name.type": "keyword",
			},
		},
	}

	result.ComponentTemplates = []olivere.IndicesGetComponentTemplates{{ComponentTemplate: component}}

	httpmock.RegisterResponder("GET", urlComponentTemplate, func(req *http.Request) (*http.Response, error) {
		resp, err := httpmock.NewJsonResponse(200, result)
		if err != nil {
			panic(err)
		}
		SetHeaders(resp)
		return resp, nil
	})

	resp, err := t.esHandler.ComponentTemplateGet("test")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), component, resp)

	// When error
	httpmock.RegisterResponder("GET", urlComponentTemplate, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.ComponentTemplateGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestComponentTemplateDelete() {

	httpmock.RegisterResponder("DELETE", urlComponentTemplate, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.ComponentTemplateDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("DELETE", urlComponentTemplate, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.ComponentTemplateDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestComponentTemplateUpdate() {
	component := &olivere.IndicesGetComponentTemplate{
		Template: &olivere.IndicesGetComponentTemplateData{
			Settings: map[string]any{
				"index.refresh_interval": "5s",
			},
			Mappings: map[string]any{
				"_source.enabled":           true,
				"properties.host_name.type": "keyword",
			},
		},
	}

	httpmock.RegisterResponder("PUT", urlComponentTemplate, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.ComponentTemplateUpdate("test", component)
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlComponentTemplate, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.ComponentTemplateUpdate("test", component)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestComponentTemplateDiff() {
	var actual, expected, original *olivere.IndicesGetComponentTemplate

	expected = &olivere.IndicesGetComponentTemplate{
		Template: &olivere.IndicesGetComponentTemplateData{
			Settings: map[string]any{
				"index.refresh_interval": "5s",
			},
			Mappings: map[string]any{
				"_source.enabled":           true,
				"properties.host_name.type": "keyword",
			},
		},
	}

	// When component not exist yet
	actual = nil
	diff, err := t.esHandler.ComponentTemplateDiff(actual, expected, nil)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When component is the same
	actual = &olivere.IndicesGetComponentTemplate{
		Template: &olivere.IndicesGetComponentTemplateData{
			Settings: map[string]any{
				"index.refresh_interval": "5s",
			},
			Mappings: map[string]any{
				"_source.enabled":           true,
				"properties.host_name.type": "keyword",
			},
		},
	}
	diff, err = t.esHandler.ComponentTemplateDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When component is not the same
	expected.Template.Mappings = map[string]any{
		"_source.enabled":           false,
		"properties.host_name.type": "keyword",
	}
	diff, err = t.esHandler.ComponentTemplateDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When elastic add default value
	actual = &olivere.IndicesGetComponentTemplate{
		Template: &olivere.IndicesGetComponentTemplateData{
			Settings: map[string]any{
				"index.refresh_interval": "5s",
			},
			Mappings: map[string]any{
				"_source.enabled":           true,
				"properties.host_name.type": "keyword",
				"default":                   "test",
			},
		},
	}

	expected = &olivere.IndicesGetComponentTemplate{
		Template: &olivere.IndicesGetComponentTemplateData{
			Settings: map[string]any{
				"index.refresh_interval": "5s",
			},
			Mappings: map[string]any{
				"_source.enabled":           true,
				"properties.host_name.type": "keyword",
			},
		},
	}

	original = &olivere.IndicesGetComponentTemplate{
		Template: &olivere.IndicesGetComponentTemplateData{
			Settings: map[string]any{
				"index.refresh_interval": "5s",
			},
			Mappings: map[string]any{
				"_source.enabled":           true,
				"properties.host_name.type": "keyword",
			},
		},
	}

	diff, err = t.esHandler.ComponentTemplateDiff(actual, expected, original)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), actual, diff.Patched)

}
