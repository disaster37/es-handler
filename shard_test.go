package eshandler

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

var urlCatShards = fmt.Sprintf("%s/_cat/shards", baseURL)

const rawCatShards = `[
	{"index":"my-index","shard":"0","prirep":"p","state":"STARTED","docs":"10","store":"1kb","ip":"127.0.0.1","node":"node-1"},
	{"index":"my-index","shard":"0","prirep":"r","state":"STARTED","docs":"10","store":"1kb","ip":"127.0.0.2","node":"node-2"},
	{"index":"my-index","shard":"0","prirep":"r","state":"INITIALIZING","docs":null,"store":null,"ip":"127.0.0.3","node":"node-3"},
	{"index":"my-index","shard":"1","prirep":"p","state":"UNASSIGNED","docs":null,"store":null,"ip":null,"node":null},
	{"index":"other-index","shard":"0","prirep":"p","state":"STARTED","docs":"5","store":"1kb","ip":"127.0.0.1","node":"node-1"}
]`

func (t *ElasticsearchHandlerTestSuite) TestGetShards() {

	httpmock.RegisterResponder("GET", urlCatShards, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, rawCatShards)
		SetHeaders(resp)
		return resp, nil
	})

	shards, err := t.esHandler.GetShards(context.Background())
	if err != nil {
		t.Fail(err.Error())
	}
	if assert.Len(t.T(), shards, 5) {
		assert.Equal(t.T(), ShardInfo{Index: "my-index", Shard: "0", Primary: true, State: "STARTED", NodeName: "node-1"}, shards[0])
		assert.Equal(t.T(), ShardInfo{Index: "my-index", Shard: "0", Primary: false, State: "STARTED", NodeName: "node-2"}, shards[1])
		assert.Equal(t.T(), ShardInfo{Index: "my-index", Shard: "1", Primary: true, State: "UNASSIGNED"}, shards[3])
	}

	// When error
	httpmock.RegisterResponder("GET", urlCatShards, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.GetShards(context.Background())
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestGetShardsByNode() {

	httpmock.RegisterResponder("GET", urlCatShards, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, rawCatShards)
		SetHeaders(resp)
		return resp, nil
	})

	byNode, err := t.esHandler.GetShardsByNode(context.Background())
	if err != nil {
		t.Fail(err.Error())
	}
	// Unassigned shard (my-index/1) is not included in any group
	assert.Len(t.T(), byNode, 3)
	assert.Len(t.T(), byNode["node-1"], 2)
	assert.Len(t.T(), byNode["node-2"], 1)
	assert.Len(t.T(), byNode["node-3"], 1)

	// When error
	httpmock.RegisterResponder("GET", urlCatShards, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.GetShardsByNode(context.Background())
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestGetShardsByIndex() {

	httpmock.RegisterResponder("GET", urlCatShards, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, rawCatShards)
		SetHeaders(resp)
		return resp, nil
	})

	byIndex, err := t.esHandler.GetShardsByIndex(context.Background())
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Len(t.T(), byIndex, 2)
	assert.Len(t.T(), byIndex["my-index"], 4)
	assert.Len(t.T(), byIndex["other-index"], 1)

	// When error
	httpmock.RegisterResponder("GET", urlCatShards, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.GetShardsByIndex(context.Background())
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestHasShardActivity() {

	urlHealth := fmt.Sprintf("%s/health", urlCluster)

	httpmock.RegisterResponder("GET", urlHealth, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{
			"cluster_name": "test",
			"status": "yellow",
			"timed_out": false,
			"number_of_nodes": 3,
			"number_of_data_nodes": 3,
			"active_primary_shards": 2,
			"active_shards": 3,
			"relocating_shards": 0,
			"initializing_shards": 1,
			"unassigned_shards": 1
		}`)
		SetHeaders(resp)
		return resp, nil
	})

	hasActivity, err := t.esHandler.HasShardActivity(context.Background())
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), hasActivity)

	// When no activity
	httpmock.RegisterResponder("GET", urlHealth, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{
			"cluster_name": "test",
			"status": "green",
			"timed_out": false,
			"number_of_nodes": 3,
			"number_of_data_nodes": 3,
			"active_primary_shards": 2,
			"active_shards": 4,
			"relocating_shards": 0,
			"initializing_shards": 0,
			"unassigned_shards": 0
		}`)
		SetHeaders(resp)
		return resp, nil
	})
	hasActivity, err = t.esHandler.HasShardActivity(context.Background())
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), hasActivity)

	// When error
	httpmock.RegisterResponder("GET", urlHealth, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.HasShardActivity(context.Background())
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestCountStartedReplicas() {

	httpmock.RegisterResponder("GET", urlCatShards, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, rawCatShards)
		SetHeaders(resp)
		return resp, nil
	})

	// my-index/0 has 2 replicas: 1 STARTED (node-2), 1 INITIALIZING (node-3)
	total, started, err := t.esHandler.CountStartedReplicas(context.Background(), "my-index", "0", "")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), 2, total)
	assert.Equal(t.T(), 1, started)

	// Exclude node-2: only the INITIALIZING replica on node-3 remains
	total, started, err = t.esHandler.CountStartedReplicas(context.Background(), "my-index", "0", "node-2")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), 1, total)
	assert.Equal(t.T(), 0, started)

	// Unknown shard
	total, started, err = t.esHandler.CountStartedReplicas(context.Background(), "my-index", "42", "")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), 0, total)
	assert.Equal(t.T(), 0, started)

	// When error
	httpmock.RegisterResponder("GET", urlCatShards, httpmock.NewErrorResponder(errors.New("fack error")))
	_, _, err = t.esHandler.CountStartedReplicas(context.Background(), "my-index", "0", "")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestShardInfoKey() {
	shard := ShardInfo{Index: "my-index", Shard: "0"}
	assert.Equal(t.T(), "my-index-0", shard.Key())
}

func (t *ElasticsearchHandlerTestSuite) TestShardInfoIsPrimary() {
	assert.True(t.T(), ShardInfo{Primary: true}.IsPrimary())
	assert.False(t.T(), ShardInfo{Primary: false}.IsPrimary())
}
