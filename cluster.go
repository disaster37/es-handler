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

// setRoutingSettingTransient persists a transient cluster setting with the given value.
func (h *ElasticsearchHandlerImpl) setRoutingSettingTransient(key, value string) error {
	settings := map[string]interface{}{
		"transient": map[string]interface{}{
			key: value,
		},
	}
	_, err := h.client.Cluster().PutSettings(context.Background(), settings, nil)
	return err
}

// DisableReplicaShardsAllocationTransient sets cluster.routing.allocation.enable=primaries (transient)
// Only primary shards can be allocated; replica shards are not allocated.
// Use during rolling restart to avoid replica replication to other nodes.
func (h *ElasticsearchHandlerImpl) DisableReplicaShardsAllocationTransient() (err error) {
	return h.setRoutingSettingTransient("cluster.routing.allocation.enable", "primaries")
}

// EnableShardAllocationTransient sets cluster.routing.allocation.enable=all (transient)
// Re-enables full shard allocation after rolling restart.
func (h *ElasticsearchHandlerImpl) EnableShardAllocationTransient() (err error) {
	return h.setRoutingSettingTransient("cluster.routing.allocation.enable", "all")
}

// DisableRoutingRebalanceTransient sets cluster.routing.rebalance.enable=none (transient)
// Prevents shard rebalancing during rolling restart (avoids unnecessary shard movement).
func (h *ElasticsearchHandlerImpl) DisableRoutingRebalanceTransient() (err error) {
	return h.setRoutingSettingTransient("cluster.routing.rebalance.enable", "none")
}

// EnableRoutingRebalanceTransient sets cluster.routing.rebalance.enable=all (transient)
// Re-enables shard rebalancing after rolling restart.
func (h *ElasticsearchHandlerImpl) EnableRoutingRebalanceTransient() (err error) {
	return h.setRoutingSettingTransient("cluster.routing.rebalance.enable", "all")
}

// RemoveTransientAllocationSettings removes transient allocation and rebalance settings.
// Use to clean up after rolling restart or when migrating to Node Shutdown API.
func (h *ElasticsearchHandlerImpl) RemoveTransientAllocationSettings() (err error) {
	settings := map[string]interface{}{
		"transient": map[string]interface{}{
			"cluster.routing.allocation.enable": nil,
			"cluster.routing.rebalance.enable":  nil,
		},
	}
	_, err = h.client.Cluster().PutSettings(context.Background(), settings, nil)
	return err
}

// GetClusterRoutingAllocation retrieves current transient allocation settings.
func (h *ElasticsearchHandlerImpl) GetClusterRoutingAllocation() (allocationEnable, rebalanceEnable string, err error) {
	resp, err := h.client.Cluster().GetSettings(context.Background(), nil)
	if err != nil {
		return "", "", err
	}
	// Parse transient settings from response
	// Response structure: {"transient": {"cluster": {"routing": {"allocation": {"enable": "..."}, "rebalance": {"enable": "..."}}}}}
	if transient, ok := resp.Transient["cluster"].(map[string]interface{}); ok {
		if routing, ok := transient["routing"].(map[string]interface{}); ok {
			if alloc, ok := routing["allocation"].(map[string]interface{}); ok {
				if enable, ok := alloc["enable"].(string); ok {
					allocationEnable = enable
				}
			}
			if rebal, ok := routing["rebalance"].(map[string]interface{}); ok {
				if enable, ok := rebal["enable"].(string); ok {
					rebalanceEnable = enable
				}
			}
		}
	}
	return allocationEnable, rebalanceEnable, nil
}

// ClusterInfo contains basic cluster information.
type ClusterInfo struct {
	Name    string
	Version string
	Tagline string
}

// GetClusterInfo retrieves basic cluster information including version.
func (h *ElasticsearchHandlerImpl) GetClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	resp, err := h.client.Info().Info(ctx)
	if err != nil {
		return nil, err
	}
	info := &ClusterInfo{
		Name:    resp.ClusterName,
		Tagline: resp.Tagline,
	}
	if resp.Version != nil {
		info.Version = resp.Version.Number
	}
	return info, nil
}

// GetVersion returns just the Elasticsearch version string.
func (h *ElasticsearchHandlerImpl) GetVersion(ctx context.Context) (string, error) {
	info, err := h.GetClusterInfo(ctx)
	if err != nil {
		return "", err
	}
	return info.Version, nil
}

// Flush requests a flush on all indices.
// Optimizes index recovery when nodes restart.
func (h *ElasticsearchHandlerImpl) Flush(ctx context.Context) (err error) {
	_, err = h.client.Indices().Flush(ctx, nil, nil)
	return err
}

// SyncedFlush requests a synced flush (deprecated in ES 8.0, removed in 9.0).
// Best-effort: falls back to a regular flush.
func (h *ElasticsearchHandlerImpl) SyncedFlush(ctx context.Context) (err error) {
	return h.Flush(ctx)
}
