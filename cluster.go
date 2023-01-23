package eshandler

import (
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
