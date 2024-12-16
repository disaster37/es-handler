package eshandler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	olivere "github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
)

// ClusterHealth permit to get the cluster health
func (h *ElasticsearchHandlerImpl) ClusterHealth() (health *olivere.ClusterHealthResponse, err error) {

	res, err := h.client.API.Cluster.Health(
		h.client.API.Cluster.Health.WithContext(context.Background()),
		h.client.API.Cluster.Health.WithPretty(),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, errors.Errorf("Error when get cluster health: %s", res.String())
	}
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	h.log.Debugf("Get cluster health successfully:\n%s", string(b))

	health = &olivere.ClusterHealthResponse{}
	err = json.Unmarshal(b, health)
	if err != nil {
		return nil, err
	}

	return health, nil
}

// EnableRoutingRebalance permit to enable cluster routing rebalance
// It put `cluster.routing.rebalance.enable` to all
func (h *ElasticsearchHandlerImpl) EnableRoutingRebalance() (err error) {
	settings := map[string]interface{}{
		"persistent": map[string]interface{}{
			"cluster.routing.rebalance.enable": "all",
		},
	}

	data, err := json.Marshal(settings)
	if err != nil {
		return err
	}

	res, err := h.client.Cluster.PutSettings(
		bytes.NewReader(data),
		h.client.Cluster.PutSettings.WithPretty(),
	)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		return errors.Errorf("Error when set Elasticsearch cluster setting: %s", res.String())
	}

	return nil
}

// DisableRoutingRebalance permit to disable cluster routing rebalance
// It put `cluster.routing.rebalance.enable` to none
func (h *ElasticsearchHandlerImpl) DisableRoutingRebalance() (err error) {
	settings := map[string]interface{}{
		"persistent": map[string]interface{}{
			"cluster.routing.rebalance.enable": "none",
		},
	}

	data, err := json.Marshal(settings)
	if err != nil {
		return err
	}

	res, err := h.client.Cluster.PutSettings(
		bytes.NewReader(data),
		h.client.Cluster.PutSettings.WithPretty(),
	)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		return errors.Errorf("Error when set Elasticsearch cluster setting: %s", res.String())
	}

	return nil
}

// EnableRoutingAllocation permit to enable cluster routing allocation
// It put `cluster.routing.allocation.enable` to all
func (h *ElasticsearchHandlerImpl) EnableRoutingAllocation() (err error) {
	settings := map[string]interface{}{
		"persistent": map[string]interface{}{
			"cluster.routing.allocation.enable": "all",
		},
	}

	data, err := json.Marshal(settings)
	if err != nil {
		return err
	}

	res, err := h.client.Cluster.PutSettings(
		bytes.NewReader(data),
		h.client.Cluster.PutSettings.WithPretty(),
	)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		return errors.Errorf("Error when set Elasticsearch cluster setting: %s", res.String())
	}

	return nil
}

// DisableRoutingAllocation permit to disable cluster routing allocation
// It put `cluster.routing.allocation.enable` to primaries
func (h *ElasticsearchHandlerImpl) DisableRoutingAllocation() (err error) {
	settings := map[string]interface{}{
		"persistent": map[string]interface{}{
			"cluster.routing.allocation.enable": "primaries",
		},
	}

	data, err := json.Marshal(settings)
	if err != nil {
		return err
	}

	res, err := h.client.Cluster.PutSettings(
		bytes.NewReader(data),
		h.client.Cluster.PutSettings.WithPretty(),
	)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		return errors.Errorf("Error when set Elasticsearch cluster setting: %s", res.String())
	}

	return nil
}
