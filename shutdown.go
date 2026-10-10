package eshandler

import (
	"context"
)

// ShutdownType represents the type of node shutdown.
type ShutdownType string

const (
	// ShutdownTypeRestart indicates a temporary shutdown (node will return).
	ShutdownTypeRestart ShutdownType = "restart"
	// ShutdownTypeRemove indicates a permanent shutdown (node will not return).
	ShutdownTypeRemove ShutdownType = "remove"
)

// ShutdownStatus represents the status of a node shutdown.
type ShutdownStatus string

const (
	ShutdownNotStarted ShutdownStatus = "NOT_STARTED"
	ShutdownInProgress ShutdownStatus = "IN_PROGRESS"
	ShutdownComplete   ShutdownStatus = "COMPLETE"
	ShutdownStalled    ShutdownStatus = "STALLED"
)

// NodeShutdownInfo contains information about a node shutdown.
type NodeShutdownInfo struct {
	NodeID            string
	Type              string
	Reason            string
	Status            ShutdownStatus
	AllocationDelay   string
	ShutdownStarted   string
	ShutdownCompleted string
}

// PutNodeShutdown requests a node shutdown.
// Available since Elasticsearch 7.15.2.
// shutdownType: "restart" (temporary) or "remove" (permanent)
// reason: arbitrary string for tracking (e.g., "rolling-restart", resource version)
func (h *ElasticsearchHandlerImpl) PutNodeShutdown(ctx context.Context, nodeID string, shutdownType ShutdownType, reason string) (err error) {
	body := map[string]interface{}{
		"type":   string(shutdownType),
		"reason": reason,
	}
	_, err = h.client.Shutdown().PutNode(ctx, nodeID, body)
	return err
}

// GetNodeShutdown retrieves the shutdown status for a specific node.
// Returns nil if no shutdown is in progress for the node.
func (h *ElasticsearchHandlerImpl) GetNodeShutdown(ctx context.Context, nodeID string) (*NodeShutdownInfo, error) {
	resp, err := h.client.Shutdown().GetNode(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	if resp == nil || len(resp.Nodes) == 0 {
		return nil, nil
	}
	node := resp.Nodes[0]
	info := &NodeShutdownInfo{
		NodeID:            node.NodeID,
		Type:              node.Type,
		Reason:            node.Reason,
		AllocationDelay:   node.AllocationDelay,
		ShutdownStarted:   node.ShutdownStarted,
		ShutdownCompleted: node.ShutdownCompleted,
		Status:            shutdownStatus(node.Status),
	}
	return info, nil
}

// GetAllNodeShutdowns retrieves all ongoing node shutdowns.
func (h *ElasticsearchHandlerImpl) GetAllNodeShutdowns(ctx context.Context) ([]NodeShutdownInfo, error) {
	resp, err := h.client.Shutdown().GetNode(ctx, "")
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	shutdowns := make([]NodeShutdownInfo, 0, len(resp.Nodes))
	for _, node := range resp.Nodes {
		info := NodeShutdownInfo{
			NodeID:            node.NodeID,
			Type:              node.Type,
			Reason:            node.Reason,
			AllocationDelay:   node.AllocationDelay,
			ShutdownStarted:   node.ShutdownStarted,
			ShutdownCompleted: node.ShutdownCompleted,
			Status:            shutdownStatus(node.Status),
		}
		shutdowns = append(shutdowns, info)
	}
	return shutdowns, nil
}

// shutdownStatus extracts the shard_migration status from the node shutdown status map.
func shutdownStatus(status map[string]interface{}) ShutdownStatus {
	if statusMap, ok := status["shard_migration"].(map[string]interface{}); ok {
		if status, ok := statusMap["status"].(string); ok {
			return ShutdownStatus(status)
		}
	}
	return ""
}

// DeleteNodeShutdown cancels or cleans up a node shutdown.
func (h *ElasticsearchHandlerImpl) DeleteNodeShutdown(ctx context.Context, nodeID string) (err error) {
	_, err = h.client.Shutdown().DeleteNode(ctx, nodeID)
	return err
}
