package eshandler

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	elasticsearch "github.com/disaster37/elasticsearch/v9"
	localpatch "github.com/disaster37/es-handler/v9/patch"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

var urlComponentTemplate = fmt.Sprintf("%s/_component_template/test", baseURL)

const rawComponentTemplateResp = `{
	"component_templates": [
		{
			"name": "test",
			"component_template": {
				"template": {
					"settings": {
						"index.refresh_interval": "5s"
					},
					"mappings": {
						"_source.enabled": true,
						"properties.host_name.type": "keyword"
					}
				}
			}
		}
	]
}`

func newTestComponentTemplate() *localpatch.ComponentTemplate {
	return &localpatch.ComponentTemplate{
		Template: &localpatch.ComponentTemplateData{
			Settings: map[string]any{
				"index.refresh_interval": "5s",
			},
			Mappings: map[string]any{
				"_source.enabled":           true,
				"properties.host_name.type": "keyword",
			},
		},
	}
}

func (t *ElasticsearchHandlerTestSuite) TestComponentTemplateGet() {

	component := newTestComponentTemplate()

	httpmock.RegisterResponder("GET", urlComponentTemplate, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, rawComponentTemplateResp)
		SetHeaders(resp)
		return resp, nil
	})

	resp, err := t.esHandler.ComponentTemplateGet("test")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), component, resp)

	// When not found
	httpmock.RegisterResponder("GET", urlComponentTemplate, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	resp, err = t.esHandler.ComponentTemplateGet("test")
	assert.NoError(t.T(), err)
	assert.Nil(t.T(), resp)

	// When error
	httpmock.RegisterResponder("GET", urlComponentTemplate, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.ComponentTemplateGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestComponentTemplateDelete() {

	httpmock.RegisterResponder("DELETE", urlComponentTemplate, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.ComponentTemplateDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When not found
	httpmock.RegisterResponder("DELETE", urlComponentTemplate, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	err = t.esHandler.ComponentTemplateDelete("test")
	assert.NoError(t.T(), err)

	// When empty name
	err = t.esHandler.ComponentTemplateDelete("")
	assert.Error(t.T(), err)

	// When error
	httpmock.RegisterResponder("DELETE", urlComponentTemplate, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.ComponentTemplateDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestComponentTemplateUpdate() {
	component := newTestComponentTemplate()

	httpmock.RegisterResponder("PUT", urlComponentTemplate, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), `{"template":{"settings":{"index.refresh_interval":"5s"},"mappings":{"_source.enabled":true,"properties.host_name.type":"keyword"}}}`, string(b))
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.ComponentTemplateUpdate("test", component)
	if err != nil {
		t.Fail(err.Error())
	}

	// When empty name
	err = t.esHandler.ComponentTemplateUpdate("", component)
	assert.Error(t.T(), err)

	// When conflict
	httpmock.RegisterResponder("PUT", urlComponentTemplate, httpmock.NewStringResponder(409, `{"error":{"type":"version_conflict_engine_exception","reason":"conflict"},"status":409}`))
	err = t.esHandler.ComponentTemplateUpdate("test", component)
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsConflict(err))

	// When error
	httpmock.RegisterResponder("PUT", urlComponentTemplate, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.ComponentTemplateUpdate("test", component)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestComponentTemplateDiff() {
	var actual, expected, original *localpatch.ComponentTemplate

	expected = newTestComponentTemplate()

	// When component not exist yet
	actual = nil
	diff, err := t.esHandler.ComponentTemplateDiff(actual, expected, nil)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When component is the same
	actual = newTestComponentTemplate()
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
	actual = &localpatch.ComponentTemplate{
		Template: &localpatch.ComponentTemplateData{
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

	expected = newTestComponentTemplate()

	original = newTestComponentTemplate()

	diff, err = t.esHandler.ComponentTemplateDiff(actual, expected, original)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), actual, diff.Patched)

}
