package eshandler

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/jarcoal/httpmock"
	olivere "github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
)

var urlIndexIngestPipeline = fmt.Sprintf("%s/_ingest/pipeline/test", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestIngestPipelineGet() {

	result := olivere.IngestGetPipelineResponse{}
	pipeline := &olivere.IngestGetPipeline {
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

	// When error
	httpmock.RegisterResponder("GET", urlIndexIngestPipeline, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.IngestPipelineGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestIngestPilelineDelete() {

	httpmock.RegisterResponder("DELETE", urlIndexIngestPipeline, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.IngestPipelineDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("DELETE", urlIndexIngestPipeline, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.IngestPipelineDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestIngestPipelineUpdate() {
	pipeline := &olivere.IngestGetPipeline {
		Description: "test",
	}

	httpmock.RegisterResponder("PUT", urlIndexIngestPipeline, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.IngestPipelineUpdate("test", pipeline)
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlIndexIngestPipeline, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.IngestPipelineUpdate("test", pipeline)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestIngestPipelineDiff() {
	var actual, expected *olivere.IngestGetPipeline 

	expected = &olivere.IngestGetPipeline {
		Description: "test",
		Version: 0,
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
	diff, err := t.esHandler.IngestPipelineDiff(actual, expected)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.NotEmpty(t.T(), diff)

	// When pipeline is the same
	actual = &olivere.IngestGetPipeline {
		Description: "test",
		Version: 0,
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
	diff, err = t.esHandler.IngestPipelineDiff(actual, expected)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Empty(t.T(), diff)

	// When pipeline is not the same
	expected.Processors = []map[string]any{
		{
			"test3": "plop3",
		},
	}
	diff, err = t.esHandler.IngestPipelineDiff(actual, expected)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.NotEmpty(t.T(), diff)

}
