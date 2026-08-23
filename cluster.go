package eshandler

import (
	"context"

	esapi "github.com/disaster37/elasticsearch/v9/api"
)

// ClusterHealth permit to get the cluster health
func (h *ElasticsearchHandlerImpl) ClusterHealth() (health *esapi.ClusterHealthResponse, err error) {
	return h.client.Cluster().Health(context.Background(), nil, nil)
}

// EnableRoutingRebalance permit to enable cluster routing rebalance
// It put `cluster.routing.rebalance.enable` to all
func (h *ElasticsearchHandlerImpl) EnableRoutingRebalance() (err error) {
	return h.setRoutingSetting("cluster.routing.rebalance.enable", "all")
}

// DisableRoutingRebalance permit to disable cluster routing rebalance
// It put `cluster.routing.rebalance.enable` to none
func (h *ElasticsearchHandlerImpl) DisableRoutingRebalance() (err error) {
	return h.setRoutingSetting("cluster.routing.rebalance.enable", "none")
}

// EnableRoutingAllocation permit to enable cluster routing allocation
// It put `cluster.routing.allocation.enable` to all
func (h *ElasticsearchHandlerImpl) EnableRoutingAllocation() (err error) {
	return h.setRoutingSetting("cluster.routing.allocation.enable", "all")
}

// DisableRoutingAllocation permit to disable cluster routing allocation
// It put `cluster.routing.allocation.enable` to primaries
func (h *ElasticsearchHandlerImpl) DisableRoutingAllocation() (err error) {
	return h.setRoutingSetting("cluster.routing.allocation.enable", "primaries")
}

// setRoutingSetting persists a persistent cluster setting with the given value.
func (h *ElasticsearchHandlerImpl) setRoutingSetting(key, value string) error {
	settings := map[string]interface{}{
		"persistent": map[string]interface{}{
			key: value,
		},
	}
	_, err := h.client.Cluster().PutSettings(context.Background(), settings, nil)
	return err
}
