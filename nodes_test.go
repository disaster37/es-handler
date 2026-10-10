package eshandler

import (
	"context"
	"errors"
	"net/http"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

const rawNodesInfo = `{
	"cluster_name": "test-cluster",
	"nodes": {
		"node-id-1": {
			"name": "node-1",
			"transport_address": "127.0.0.1:9300",
			"host": "127.0.0.1",
			"ip": "127.0.0.1",
			"version": "9.0.0",
			"roles": ["master", "data"],
			"attributes": {}
		},
		"node-id-2": {
			"name": "node-2",
			"transport_address": "127.0.0.2:9300",
			"host": "127.0.0.2",
			"ip": "127.0.0.2",
			"version": "9.0.0",
			"roles": ["data"],
			"attributes": {}
		}
	}
}`

func (t *ElasticsearchHandlerTestSuite) TestGetNodesInfo() {

	httpmock.RegisterResponder("GET", urlNodes, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, rawNodesInfo)
		SetHeaders(resp)
		return resp, nil
	})

	nodes, err := t.esHandler.GetNodesInfo(context.Background())
	if err != nil {
		t.Fail(err.Error())
	}
	if assert.Len(t.T(), nodes, 2) {
		node1, ok := nodes["node-1"]
		if assert.True(t.T(), ok) {
			assert.Equal(t.T(), "node-id-1", node1.ID)
			assert.Equal(t.T(), "node-1", node1.Name)
			assert.Equal(t.T(), "9.0.0", node1.Version)
			assert.Equal(t.T(), []string{"master", "data"}, node1.Roles)
			assert.Equal(t.T(), "127.0.0.1", node1.Host)
			assert.Equal(t.T(), "127.0.0.1", node1.IP)
		}
		_, ok = nodes["node-2"]
		assert.True(t.T(), ok)
	}

	// When error
	httpmock.RegisterResponder("GET", urlNodes, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.GetNodesInfo(context.Background())
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestGetNodeNameToIDMap() {

	httpmock.RegisterResponder("GET", urlNodes, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, rawNodesInfo)
		SetHeaders(resp)
		return resp, nil
	})

	nameToID, err := t.esHandler.GetNodeNameToIDMap(context.Background())
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), map[string]string{
		"node-1": "node-id-1",
		"node-2": "node-id-2",
	}, nameToID)

	// When error
	httpmock.RegisterResponder("GET", urlNodes, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.GetNodeNameToIDMap(context.Background())
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestIsNodeInCluster() {

	httpmock.RegisterResponder("GET", urlNodes, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, rawNodesInfo)
		SetHeaders(resp)
		return resp, nil
	})

	exists, err := t.esHandler.IsNodeInCluster(context.Background(), "node-1")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), exists)

	exists, err = t.esHandler.IsNodeInCluster(context.Background(), "node-3")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), exists)

	// When error
	httpmock.RegisterResponder("GET", urlNodes, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.IsNodeInCluster(context.Background(), "node-1")
	assert.Error(t.T(), err)
}
