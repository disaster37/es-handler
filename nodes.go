package eshandler

import (
	"context"

	esapi "github.com/disaster37/elasticsearch/v9/api"
)

// NodeInfo contains basic information about an Elasticsearch node.
type NodeInfo struct {
	ID      string
	Name    string
	Version string
	Roles   []string
	Host    string
	IP      string
}

// GetNodesInfo retrieves information about all nodes in the cluster.
// Returns a map of node name -> NodeInfo.
func (h *ElasticsearchHandlerImpl) GetNodesInfo(ctx context.Context) (map[string]NodeInfo, error) {
	resp, err := h.client.Nodes().Info(ctx, &esapi.NodesInfoRequest{})
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]NodeInfo, len(resp.Nodes))
	for id, node := range resp.Nodes {
		nodes[node.Name] = NodeInfo{
			ID:      id,
			Name:    node.Name,
			Version: node.Version,
			Roles:   node.Roles,
			Host:    node.Host,
			IP:      node.IP,
		}
	}
	return nodes, nil
}

// GetNodeNameToIDMap returns a map of node name -> node ID.
// Useful for Node Shutdown API (which requires node ID, not name).
func (h *ElasticsearchHandlerImpl) GetNodeNameToIDMap(ctx context.Context) (map[string]string, error) {
	nodes, err := h.GetNodesInfo(ctx)
	if err != nil {
		return nil, err
	}
	nameToID := make(map[string]string, len(nodes))
	for name, node := range nodes {
		nameToID[name] = node.ID
	}
	return nameToID, nil
}

// IsNodeInCluster checks if a node (by name) is currently in the cluster.
func (h *ElasticsearchHandlerImpl) IsNodeInCluster(ctx context.Context, nodeName string) (bool, error) {
	nodes, err := h.GetNodesInfo(ctx)
	if err != nil {
		return false, err
	}
	_, exists := nodes[nodeName]
	return exists, nil
}
