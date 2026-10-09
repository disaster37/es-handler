package eshandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	elasticsearch "github.com/disaster37/elasticsearch/v9"
	esapi "github.com/disaster37/elasticsearch/v9/api"
	"github.com/google/go-cmp/cmp"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

var urlCluster = fmt.Sprintf("%s/_cluster", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestClusterHealth() {

	rawHealth := `
	{
		"cluster_name" : "test",
		"status" : "green",
		"timed_out" : false,
		"number_of_nodes" : 15,
		"number_of_data_nodes" : 10,
		"active_primary_shards" : 166,
		"active_shards" : 340,
		"relocating_shards" : 0,
		"initializing_shards" : 0,
		"unassigned_shards" : 0,
		"delayed_unassigned_shards" : 0,
		"number_of_pending_tasks" : 0,
		"number_of_in_flight_fetch" : 0,
		"task_max_waiting_in_queue_millis" : 0,
		"active_shards_percent_as_number" : 100.0
	}
	`

	healthTest := &esapi.ClusterHealthResponse{}
	if err := json.Unmarshal([]byte(rawHealth), healthTest); err != nil {
		panic(err)
	}

	httpmock.RegisterResponder("GET", fmt.Sprintf("%s/health", urlCluster), func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, rawHealth)
		SetHeaders(resp)
		return resp, nil
	})

	health, err := t.esHandler.ClusterHealth()
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Empty(t.T(), cmp.Diff(healthTest, health))

	// When not found
	httpmock.RegisterResponder("GET", fmt.Sprintf("%s/health", urlCluster), httpmock.NewStringResponder(404, `{"error":{"type":"index_not_found_exception","reason":"not found"},"status":404}`))
	health, err = t.esHandler.ClusterHealth()
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsNotFound(err))

	// When unauthorized
	httpmock.RegisterResponder("GET", fmt.Sprintf("%s/health", urlCluster), httpmock.NewStringResponder(401, `{"error":{"type":"security_exception","reason":"unauthorized"},"status":401}`))
	health, err = t.esHandler.ClusterHealth()
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsUnauthorized(err))

	// When error
	httpmock.RegisterResponder("GET", fmt.Sprintf("%s/health", urlCluster), httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.ClusterHealth()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestEnableRoutingRebalance() {

	urlSetting := fmt.Sprintf("%s/settings", urlCluster)

	httpmock.RegisterResponder("PUT", urlSetting, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), `{"persistent":{"cluster.routing.rebalance.enable":"all"}}`, string(b))
		resp := httpmock.NewStringResponse(200, `{}`)
		return resp, nil
	})

	err := t.esHandler.EnableRoutingRebalance()
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlSetting, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.EnableRoutingRebalance()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestDisableRoutingRebalance() {

	urlSetting := fmt.Sprintf("%s/settings", urlCluster)

	httpmock.RegisterResponder("PUT", urlSetting, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), `{"persistent":{"cluster.routing.rebalance.enable":"none"}}`, string(b))
		resp := httpmock.NewStringResponse(200, `{}`)
		return resp, nil
	})

	err := t.esHandler.DisableRoutingRebalance()
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlSetting, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.DisableRoutingRebalance()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestEnableRoutingAllocation() {

	urlSetting := fmt.Sprintf("%s/settings", urlCluster)

	httpmock.RegisterResponder("PUT", urlSetting, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), `{"persistent":{"cluster.routing.allocation.enable":"all"}}`, string(b))
		resp := httpmock.NewStringResponse(200, `{}`)
		return resp, nil
	})

	err := t.esHandler.EnableRoutingAllocation()
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlSetting, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.EnableRoutingAllocation()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestDisableRoutingAllocation() {

	urlSetting := fmt.Sprintf("%s/settings", urlCluster)

	httpmock.RegisterResponder("PUT", urlSetting, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), `{"persistent":{"cluster.routing.allocation.enable":"primaries"}}`, string(b))
		resp := httpmock.NewStringResponse(200, `{}`)
		return resp, nil
	})

	err := t.esHandler.DisableRoutingAllocation()
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlSetting, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.DisableRoutingAllocation()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestDisableReplicaShardsAllocationTransient() {

	urlSetting := fmt.Sprintf("%s/settings", urlCluster)

	httpmock.RegisterResponder("PUT", urlSetting, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), `{"transient":{"cluster.routing.allocation.enable":"primaries"}}`, string(b))
		resp := httpmock.NewStringResponse(200, `{}`)
		return resp, nil
	})

	err := t.esHandler.DisableReplicaShardsAllocationTransient()
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlSetting, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.DisableReplicaShardsAllocationTransient()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestEnableShardAllocationTransient() {

	urlSetting := fmt.Sprintf("%s/settings", urlCluster)

	httpmock.RegisterResponder("PUT", urlSetting, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), `{"transient":{"cluster.routing.allocation.enable":"all"}}`, string(b))
		resp := httpmock.NewStringResponse(200, `{}`)
		return resp, nil
	})

	err := t.esHandler.EnableShardAllocationTransient()
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlSetting, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.EnableShardAllocationTransient()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestDisableRoutingRebalanceTransient() {

	urlSetting := fmt.Sprintf("%s/settings", urlCluster)

	httpmock.RegisterResponder("PUT", urlSetting, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), `{"transient":{"cluster.routing.rebalance.enable":"none"}}`, string(b))
		resp := httpmock.NewStringResponse(200, `{}`)
		return resp, nil
	})

	err := t.esHandler.DisableRoutingRebalanceTransient()
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlSetting, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.DisableRoutingRebalanceTransient()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestEnableRoutingRebalanceTransient() {

	urlSetting := fmt.Sprintf("%s/settings", urlCluster)

	httpmock.RegisterResponder("PUT", urlSetting, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), `{"transient":{"cluster.routing.rebalance.enable":"all"}}`, string(b))
		resp := httpmock.NewStringResponse(200, `{}`)
		return resp, nil
	})

	err := t.esHandler.EnableRoutingRebalanceTransient()
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlSetting, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.EnableRoutingRebalanceTransient()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestRemoveTransientAllocationSettings() {

	urlSetting := fmt.Sprintf("%s/settings", urlCluster)

	httpmock.RegisterResponder("PUT", urlSetting, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), `{"transient":{"cluster.routing.allocation.enable":null,"cluster.routing.rebalance.enable":null}}`, string(b))
		resp := httpmock.NewStringResponse(200, `{}`)
		return resp, nil
	})

	err := t.esHandler.RemoveTransientAllocationSettings()
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlSetting, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.RemoveTransientAllocationSettings()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestGetClusterRoutingAllocation() {

	urlSetting := fmt.Sprintf("%s/settings", urlCluster)

	httpmock.RegisterResponder("GET", urlSetting, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{
			"persistent": {},
			"transient": {
				"cluster": {
					"routing": {
						"allocation": {"enable": "primaries"},
						"rebalance": {"enable": "none"}
					}
				}
			}
		}`)
		SetHeaders(resp)
		return resp, nil
	})

	allocationEnable, rebalanceEnable, err := t.esHandler.GetClusterRoutingAllocation()
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), "primaries", allocationEnable)
	assert.Equal(t.T(), "none", rebalanceEnable)

	// When no transient settings
	httpmock.RegisterResponder("GET", urlSetting, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{"persistent": {}, "transient": {}}`)
		SetHeaders(resp)
		return resp, nil
	})
	allocationEnable, rebalanceEnable, err = t.esHandler.GetClusterRoutingAllocation()
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Empty(t.T(), allocationEnable)
	assert.Empty(t.T(), rebalanceEnable)

	// When error
	httpmock.RegisterResponder("GET", urlSetting, httpmock.NewErrorResponder(errors.New("fack error")))
	_, _, err = t.esHandler.GetClusterRoutingAllocation()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestGetClusterInfo() {

	rawInfo := `{
		"name" : "node-1",
		"cluster_name" : "test-cluster",
		"cluster_uuid" : "abc123",
		"version" : {
			"number" : "9.0.0",
			"build_flavor" : "default",
			"build_type" : "docker",
			"build_hash" : "abcdef",
			"build_date" : "2024-01-01T00:00:00.000Z",
			"build_snapshot" : false,
			"lucene_version" : "10.0.0",
			"minimum_wire_compatibility_version" : "8.0.0",
			"minimum_index_compatibility_version" : "8.0.0"
		},
		"tagline" : "You Know, for Search"
	}`

	httpmock.RegisterResponder("GET", baseURL+"/", func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, rawInfo)
		SetHeaders(resp)
		return resp, nil
	})

	info, err := t.esHandler.GetClusterInfo(context.Background())
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), "test-cluster", info.Name)
	assert.Equal(t.T(), "9.0.0", info.Version)
	assert.Equal(t.T(), "You Know, for Search", info.Tagline)

	// When unauthorized
	httpmock.RegisterResponder("GET", baseURL+"/", httpmock.NewStringResponder(401, `{"error":{"type":"security_exception","reason":"unauthorized"},"status":401}`))
	_, err = t.esHandler.GetClusterInfo(context.Background())
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsUnauthorized(err))

	// When error
	httpmock.RegisterResponder("GET", baseURL+"/", httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.GetClusterInfo(context.Background())
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestGetVersion() {

	httpmock.RegisterResponder("GET", baseURL+"/", func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{
			"name" : "node-1",
			"cluster_name" : "test-cluster",
			"cluster_uuid" : "abc123",
			"version" : {"number" : "9.0.0"},
			"tagline" : "You Know, for Search"
		}`)
		SetHeaders(resp)
		return resp, nil
	})

	version, err := t.esHandler.GetVersion(context.Background())
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), "9.0.0", version)

	// When error
	httpmock.RegisterResponder("GET", baseURL+"/", httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.GetVersion(context.Background())
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestFlush() {

	httpmock.RegisterResponder("POST", fmt.Sprintf("%s/_flush", baseURL), func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{"_shards":{"total":2,"successful":2,"failed":0}}`)
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.Flush(context.Background())
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("POST", fmt.Sprintf("%s/_flush", baseURL), httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.Flush(context.Background())
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestSyncedFlush() {

	httpmock.RegisterResponder("POST", fmt.Sprintf("%s/_flush", baseURL), func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{"_shards":{"total":2,"successful":2,"failed":0}}`)
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.SyncedFlush(context.Background())
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("POST", fmt.Sprintf("%s/_flush", baseURL), httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.SyncedFlush(context.Background())
	assert.Error(t.T(), err)
}
