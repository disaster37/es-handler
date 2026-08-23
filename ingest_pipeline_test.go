package eshandler

import (
	"errors"
	"fmt"
	"net/http"

	elasticsearch "github.com/disaster37/elasticsearch/v9"
	esapi "github.com/disaster37/elasticsearch/v9/api"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

var urlIndexIngestPipeline = fmt.Sprintf("%s/_ingest/pipeline/test", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestIngestPipelineGet() {

	result := make(map[string]*esapi.IngestPipeline)
	pipeline := &esapi.IngestPipeline{
		Description: "test",
	}
	result["test"] = pipeline

	httpmock.RegisterResponder("GET", urlIndexIngestPipeline, func(req *http.Request) (*http.Response, error) {
		resp, err := httpmock.NewJsonResponse(200, result)
		if err != nil {
			panic(err)
		}
		SetHeaders(resp)
		return resp, nil
	})

	resp, err := t.esHandler.IngestPipelineGet("test")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), pipeline, resp)

	// When not found
	httpmock.RegisterResponder("GET", urlIndexIngestPipeline, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	resp, err = t.esHandler.IngestPipelineGet("test")
	assert.NoError(t.T(), err)
	assert.Nil(t.T(), resp)

	// When error
	httpmock.RegisterResponder("GET", urlIndexIngestPipeline, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.IngestPipelineGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestIngestPilelineDelete() {

	httpmock.RegisterResponder("DELETE", urlIndexIngestPipeline, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.IngestPipelineDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When not found
	httpmock.RegisterResponder("DELETE", urlIndexIngestPipeline, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	err = t.esHandler.IngestPipelineDelete("test")
	assert.NoError(t.T(), err)

	// When empty name
	err = t.esHandler.IngestPipelineDelete("")
	assert.Error(t.T(), err)

	// When error
	httpmock.RegisterResponder("DELETE", urlIndexIngestPipeline, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.IngestPipelineDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestIngestPipelineUpdate() {
	pipeline := &esapi.IngestPipeline{
		Description: "test",
	}

	httpmock.RegisterResponder("PUT", urlIndexIngestPipeline, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.IngestPipelineUpdate("test", pipeline)
	if err != nil {
		t.Fail(err.Error())
	}

	// When empty name
	err = t.esHandler.IngestPipelineUpdate("", pipeline)
	assert.Error(t.T(), err)

	// When conflict
	httpmock.RegisterResponder("PUT", urlIndexIngestPipeline, httpmock.NewStringResponder(409, `{"error":{"type":"version_conflict_engine_exception","reason":"conflict"},"status":409}`))
	err = t.esHandler.IngestPipelineUpdate("test", pipeline)
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsConflict(err))

	// When error
	httpmock.RegisterResponder("PUT", urlIndexIngestPipeline, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.IngestPipelineUpdate("test", pipeline)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestIngestPipelineDiff() {
	var actual, expected, original *esapi.IngestPipeline

	expected = &esapi.IngestPipeline{
		Description: "test",
		Version:     0,
		Processors: []map[string]any{
			{
				"test": "plop",
			},
		},
		OnFailure: []map[string]any{
			{
				"test2": "plop2",
			},
		},
	}

	// When pipeline not exist yet
	actual = nil
	diff, err := t.esHandler.IngestPipelineDiff(actual, expected, nil)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When pipeline is the same
	actual = &esapi.IngestPipeline{
		Description: "test",
		Version:     0,
		Processors: []map[string]any{
			{
				"test": "plop",
			},
		},
		OnFailure: []map[string]any{
			{
				"test2": "plop2",
			},
		},
	}
	diff, err = t.esHandler.IngestPipelineDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When pipeline is not the same
	expected.Processors = []map[string]any{
		{
			"test3": "plop3",
		},
	}
	diff, err = t.esHandler.IngestPipelineDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When Elastic add default value
	actual = &esapi.IngestPipeline{
		Description: "test",
		Version:     10,
		Processors: []map[string]any{
			{
				"test": "plop",
			},
		},
		OnFailure: []map[string]any{
			{
				"test2": "plop2",
			},
		},
	}

	expected = &esapi.IngestPipeline{
		Description: "test",
		Processors: []map[string]any{
			{
				"test": "plop",
			},
		},
		OnFailure: []map[string]any{
			{
				"test2": "plop2",
			},
		},
	}

	original = &esapi.IngestPipeline{
		Description: "test",
		Processors: []map[string]any{
			{
				"test": "plop",
			},
		},
		OnFailure: []map[string]any{
			{
				"test2": "plop2",
			},
		},
	}

	diff, err = t.esHandler.IngestPipelineDiff(actual, expected, original)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), actual, diff.Patched)

}
