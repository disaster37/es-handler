package eshandler

import (
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
