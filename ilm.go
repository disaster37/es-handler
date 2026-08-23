package eshandler

import (
	"context"

	esapi "github.com/disaster37/elasticsearch/v9/api"
	"github.com/disaster37/generic-objectmatcher/patch"
)

// ILMUpdate permit to update or create policy
func (h *ElasticsearchHandlerImpl) ILMUpdate(name string, policy *esapi.IlmPolicy) (err error) {
	_, err = h.client.ILM().PutLifecycle(context.Background(), name, policy)
	return err
}

// ILMDelete permit to delete policy
func (h *ElasticsearchHandlerImpl) ILMDelete(name string) (err error) {
	h.log.Debugf("Name: %s", name)
	_, err = h.client.ILM().DeleteLifecycle(context.Background(), name)
	return ignoreNotFound(err)
}

// ILMGet permit to get policy
func (h *ElasticsearchHandlerImpl) ILMGet(name string) (policy *esapi.IlmPolicy, err error) {
	h.log.Debugf("Name: %s", name)
	policies, err := h.client.ILM().GetLifecycle(context.Background(), []string{name})
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	return policies[name], nil
}

// ILMDiff permit to check if 2 policy are the same
func (h *ElasticsearchHandlerImpl) ILMDiff(actualObject, expectedObject, originalObject *esapi.IlmPolicy) (patchResult *patch.PatchResult, err error) {
	return computeDiff(actualObject, expectedObject, originalObject)
}
