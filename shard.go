package eshandler

import (
	"context"
	"fmt"

	esapi "github.com/disaster37/elasticsearch/v9/api"
)

// ShardInfo represents information about a single shard.
type ShardInfo struct {
	Index    string
	Shard    string
	Primary  bool   // true = primary, false = replica
	State    string // STARTED, UNASSIGNED, INITIALIZING, RELOCATING
	NodeName string // empty if unassigned
	// NodeID is currently never populated: the Cat Shards API does not expose
	// node IDs (only node names). It is kept for forward compatibility.
	NodeID string
}

// Key returns a unique key for the shard (index-shard).
func (s ShardInfo) Key() string {
	return fmt.Sprintf("%s-%s", s.Index, s.Shard)
}

// IsPrimary returns true if this is a primary shard.
func (s ShardInfo) IsPrimary() bool {
	return s.Primary
}

// GetShards retrieves all shards in the cluster using the Cat API.
func (h *ElasticsearchHandlerImpl) GetShards(ctx context.Context) ([]ShardInfo, error) {
	resp, err := h.client.Cat().Shards(ctx, nil, &esapi.CatShardsParams{})
	if err != nil {
		return nil, err
	}
	shards := make([]ShardInfo, 0, len(resp))
	for _, s := range resp {
		shards = append(shards, ShardInfo{
			Index:    s.Index,
			Shard:    s.Shard,
			Primary:  s.Prirep == "p",
			State:    s.State,
			NodeName: s.Node,
		})
	}
	return shards, nil
}

// GetShardsByNode returns shards grouped by node name.
// Unassigned shards (NodeName == "") are not included in any group.
func (h *ElasticsearchHandlerImpl) GetShardsByNode(ctx context.Context) (map[string][]ShardInfo, error) {
	shards, err := h.GetShards(ctx)
	if err != nil {
		return nil, err
	}
	byNode := make(map[string][]ShardInfo)
	for _, shard := range shards {
		if shard.NodeName != "" {
			byNode[shard.NodeName] = append(byNode[shard.NodeName], shard)
		}
	}
	return byNode, nil
}

// GetShardsByIndex returns shards grouped by index name.
func (h *ElasticsearchHandlerImpl) GetShardsByIndex(ctx context.Context) (map[string][]ShardInfo, error) {
	shards, err := h.GetShards(ctx)
	if err != nil {
		return nil, err
	}
	byIndex := make(map[string][]ShardInfo)
	for _, shard := range shards {
		byIndex[shard.Index] = append(byIndex[shard.Index], shard)
	}
	return byIndex, nil
}

// HasShardActivity checks if there are any initializing or relocating shards.
// Uses cluster health with wait_for_events=languid for accuracy.
func (h *ElasticsearchHandlerImpl) HasShardActivity(ctx context.Context) (bool, error) {
	health, err := h.client.Cluster().Health(ctx, nil, &esapi.ClusterHealthParams{
		WaitForEvents: "languid",
		Timeout:       "0s",
	})
	if err != nil {
		return false, err
	}
	// Check for initializing or relocating shards
	return health.InitializingShards > 0 || health.RelocatingShards > 0, nil
}

// CountStartedReplicas counts STARTED replicas for a given shard key (index-shard).
// Excludes shards on the specified node (if excludeNode is not empty).
func (h *ElasticsearchHandlerImpl) CountStartedReplicas(ctx context.Context, index, shard, excludeNode string) (total, started int, err error) {
	shards, err := h.GetShards(ctx)
	if err != nil {
		return 0, 0, err
	}
	shardKey := ShardInfo{Index: index, Shard: shard}.Key()
	for _, s := range shards {
		if s.Key() != shardKey {
			continue
		}
		if s.Primary {
			continue // Skip primaries
		}
		if excludeNode != "" && s.NodeName == excludeNode {
			continue // Skip shards on excluded node
		}
		total++
		if s.State == "STARTED" {
			started++
		}
	}
	return total, started, nil
}
