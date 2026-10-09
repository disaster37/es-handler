package eshandler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

var urlNodes = fmt.Sprintf("%s/_nodes", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestPutNodeShutdown() {

	urlShutdown := fmt.Sprintf("%s/node-1/shutdown", urlNodes)

	httpmock.RegisterResponder("PUT", urlShutdown, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), `{"type":"restart","reason":"rolling-restart"}`, string(b))
		resp := httpmock.NewStringResponse(200, `{"acknowledged":true}`)
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.PutNodeShutdown(context.Background(), "node-1", ShutdownTypeRestart, "rolling-restart")
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlShutdown, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.PutNodeShutdown(context.Background(), "node-1", ShutdownTypeRestart, "rolling-restart")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestGetNodeShutdown() {

	urlShutdown := fmt.Sprintf("%s/node-1/shutdown", urlNodes)

	httpmock.RegisterResponder("GET", urlShutdown, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{
			"nodes": [
				{
					"node_id": "node-1",
					"type": "restart",
					"reason": "rolling-restart",
					"shutdown_started": "2024-01-01T00:00:00.000Z",
					"allocation_delay": "1m",
					"status": {
						"shard_migration": {"status": "IN_PROGRESS"}
					}
				}
			]
		}`)
		SetHeaders(resp)
		return resp, nil
	})

	info, err := t.esHandler.GetNodeShutdown(context.Background(), "node-1")
	if err != nil {
		t.Fail(err.Error())
	}
	if assert.NotNil(t.T(), info) {
		assert.Equal(t.T(), "node-1", info.NodeID)
		assert.Equal(t.T(), "restart", info.Type)
		assert.Equal(t.T(), "rolling-restart", info.Reason)
		assert.Equal(t.T(), ShutdownInProgress, info.Status)
		assert.Equal(t.T(), "1m", info.AllocationDelay)
		assert.Equal(t.T(), "2024-01-01T00:00:00.000Z", info.ShutdownStarted)
	}

	// When no shutdown in progress
	httpmock.RegisterResponder("GET", urlShutdown, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{"nodes":[]}`)
		SetHeaders(resp)
		return resp, nil
	})
	info, err = t.esHandler.GetNodeShutdown(context.Background(), "node-1")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Nil(t.T(), info)

	// When error
	httpmock.RegisterResponder("GET", urlShutdown, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.GetNodeShutdown(context.Background(), "node-1")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestGetAllNodeShutdowns() {

	urlShutdown := fmt.Sprintf("%s/shutdown", urlNodes)

	httpmock.RegisterResponder("GET", urlShutdown, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{
			"nodes": [
				{
					"node_id": "node-1",
					"type": "restart",
					"reason": "rolling-restart",
					"shutdown_started": "2024-01-01T00:00:00.000Z",
					"status": {
						"shard_migration": {"status": "COMPLETE"}
					}
				},
				{
					"node_id": "node-2",
					"type": "remove",
					"reason": "decommission",
					"shutdown_started": "2024-01-01T01:00:00.000Z",
					"shutdown_completed": "2024-01-01T01:05:00.000Z",
					"status": {
						"shard_migration": {"status": "STALLED"}
					}
				}
			]
		}`)
		SetHeaders(resp)
		return resp, nil
	})

	shutdowns, err := t.esHandler.GetAllNodeShutdowns(context.Background())
	if err != nil {
		t.Fail(err.Error())
	}
	if assert.Len(t.T(), shutdowns, 2) {
		assert.Equal(t.T(), "node-1", shutdowns[0].NodeID)
		assert.Equal(t.T(), "restart", shutdowns[0].Type)
		assert.Equal(t.T(), ShutdownComplete, shutdowns[0].Status)
		assert.Equal(t.T(), "node-2", shutdowns[1].NodeID)
		assert.Equal(t.T(), "remove", shutdowns[1].Type)
		assert.Equal(t.T(), ShutdownStalled, shutdowns[1].Status)
		assert.Equal(t.T(), "2024-01-01T01:05:00.000Z", shutdowns[1].ShutdownCompleted)
	}

	// When no shutdown in progress
	httpmock.RegisterResponder("GET", urlShutdown, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{"nodes":[]}`)
		SetHeaders(resp)
		return resp, nil
	})
	shutdowns, err = t.esHandler.GetAllNodeShutdowns(context.Background())
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Empty(t.T(), shutdowns)

	// When error
	httpmock.RegisterResponder("GET", urlShutdown, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.GetAllNodeShutdowns(context.Background())
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestDeleteNodeShutdown() {

	urlShutdown := fmt.Sprintf("%s/node-1/shutdown", urlNodes)

	httpmock.RegisterResponder("DELETE", urlShutdown, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{"acknowledged":true}`)
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.DeleteNodeShutdown(context.Background(), "node-1")
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("DELETE", urlShutdown, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.DeleteNodeShutdown(context.Background(), "node-1")
	assert.Error(t.T(), err)
}
