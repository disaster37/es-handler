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

var urlIndexTemplate = fmt.Sprintf("%s/_index_template/test", baseURL)

const rawIndexTemplateResp = `{
	"index_templates": [
		{
			"name": "test",
			"index_template": {
				"index_patterns": ["test-index-template"],
				"priority": 2,
				"template": {
					"settings": {
						"index.refresh_interval": "5s"
					}
				}
			}
		}
	]
}`

func newTestIndexTemplate() *localpatch.IndexTemplate {
	return &localpatch.IndexTemplate{
		IndexPatterns: []string{"test-index-template"},
		Priority:      2,
		Template: &localpatch.IndexTemplateData{
			Settings: map[string]any{
				"index.refresh_interval": "5s",
			},
		},
	}
}

func (t *ElasticsearchHandlerTestSuite) TestIndexTemplateGet() {

	template := newTestIndexTemplate()

	httpmock.RegisterResponder("GET", urlIndexTemplate, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, rawIndexTemplateResp)
		SetHeaders(resp)
		return resp, nil
	})

	resp, err := t.esHandler.IndexTemplateGet("test")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), template, resp)

	// When not found
	httpmock.RegisterResponder("GET", urlIndexTemplate, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	resp, err = t.esHandler.IndexTemplateGet("test")
	assert.NoError(t.T(), err)
	assert.Nil(t.T(), resp)

	// When error
	httpmock.RegisterResponder("GET", urlIndexTemplate, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.IndexTemplateGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestIndexTemplateDelete() {

	httpmock.RegisterResponder("DELETE", urlIndexTemplate, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.IndexTemplateDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When not found
	httpmock.RegisterResponder("DELETE", urlIndexTemplate, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	err = t.esHandler.IndexTemplateDelete("test")
	assert.NoError(t.T(), err)

	// When empty name
	err = t.esHandler.IndexTemplateDelete("")
	assert.Error(t.T(), err)

	// When error
	httpmock.RegisterResponder("DELETE", urlIndexTemplate, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.IndexTemplateDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestIndexTemplateUpdate() {
	template := newTestIndexTemplate()

	httpmock.RegisterResponder("PUT", urlIndexTemplate, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), `{"index_patterns":["test-index-template"],"priority":2,"template":{"settings":{"index.refresh_interval":"5s"}}}`, string(b))
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.IndexTemplateUpdate("test", template)
	if err != nil {
		t.Fail(err.Error())
	}

	// When empty name
	err = t.esHandler.IndexTemplateUpdate("", template)
	assert.Error(t.T(), err)

	// When conflict
	httpmock.RegisterResponder("PUT", urlIndexTemplate, httpmock.NewStringResponder(409, `{"error":{"type":"version_conflict_engine_exception","reason":"conflict"},"status":409}`))
	err = t.esHandler.IndexTemplateUpdate("test", template)
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsConflict(err))

	// When error
	httpmock.RegisterResponder("PUT", urlIndexTemplate, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.IndexTemplateUpdate("test", template)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestIndexTemplateDiff() {
	var actual, expected, original *localpatch.IndexTemplate

	expected = newTestIndexTemplate()

	// When template not exist yet
	actual = nil
	diff, err := t.esHandler.IndexTemplateDiff(actual, expected, nil)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When template is the same
	actual = newTestIndexTemplate()
	diff, err = t.esHandler.IndexTemplateDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When template is not the same
	expected.Template = &localpatch.IndexTemplateData{
		Mappings: map[string]any{
			"_source.enabled":           false,
			"properties.host_name.type": "keyword",
		},
	}
	diff, err = t.esHandler.IndexTemplateDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When Elastic add default value
	actual = &localpatch.IndexTemplate{
		IndexPatterns: []string{"test-index-template"},
		Priority:      2,
		Template: &localpatch.IndexTemplateData{
			Settings: map[string]any{
				"index.refresh_interval": "5s",
			},
		},
		Meta: map[string]interface{}{
			"default": "test",
		},
	}

	expected = newTestIndexTemplate()

	original = newTestIndexTemplate()

	diff, err = t.esHandler.IndexTemplateDiff(actual, expected, original)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), actual, diff.Patched)

}
