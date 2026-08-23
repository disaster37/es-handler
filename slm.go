package eshandler

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/disaster37/generic-objectmatcher/patch"
)

// SnapshotLifecyclePolicy object returned by API
type SnapshotLifecyclePolicy map[string]*SnapshotLifecyclePolicyGet

// SnapshotLifecyclePolicySpec is the snapshot lifecycle policy object
type SnapshotLifecyclePolicySpec struct {
	Schedule   string                     `json:"schedule"`
	Name       string                     `json:"name"`
	Repository string                     `json:"repository"`
	Config     ElasticsearchSLMConfig     `json:"config"`
	Retention  *ElasticsearchSLMRetention `json:"retention,omitempty"`
}

// ElasticsearchSLMConfig is the config sub section
type ElasticsearchSLMConfig struct {
	ExpendWildcards    string            `json:"expand_wildcards,omitempty"`
	IgnoreUnavailable  bool              `json:"ignore_unavailable,omitempty"`
	IncludeGlobalState bool              `json:"include_global_state,omitempty"`
	Indices            []string          `json:"indices,omitempty"`
	FeatureStates      []string          `json:"feature_states,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	Partial            bool              `json:"partial,omitempty"`
}

// ElasticsearchSLMRetention is the retention sub section
type ElasticsearchSLMRetention struct {
	ExpireAfter string `json:"expire_after,omitempty"`
	MaxCount    int64  `json:"max_count,omitempty"`
	MinCount    int64  `json:"min_count,omitempty"`
}

// SnapshotLifecyclePolicyGet is the policy
type SnapshotLifecyclePolicyGet struct {
	Policy *SnapshotLifecyclePolicySpec `json:"policy"`
}

// SLMUpdate permit to add or update SLM policy
func (h *ElasticsearchHandlerImpl) SLMUpdate(name string, policy *SnapshotLifecyclePolicySpec) (err error) {
	_, err = h.client.SLM().PutLifecycle(context.Background(), name, policy)
	return err
}

// SLMDelete permit to delete SLM policy
func (h *ElasticsearchHandlerImpl) SLMDelete(name string) (err error) {
	_, err = h.client.SLM().DeleteLifecycle(context.Background(), name)
	return ignoreNotFound(err)
}

// SLMGet permit to get SLM policy
func (h *ElasticsearchHandlerImpl) SLMGet(name string) (policy *SnapshotLifecyclePolicySpec, err error) {
	b, err := h.getRaw("/_slm/policy/" + url.PathEscape(name))
	if err != nil || b == nil {
		return nil, err
	}

	slm := make(SnapshotLifecyclePolicy)
	if err = json.Unmarshal(b, &slm); err != nil {
		return nil, err
	}

	h.log.Debugf("Get snapshot lifecycle policy successfully:\n%s", string(b))

	// Manage bug https://github.com/elastic/elasticsearch/issues/47664
	if len(slm) == 0 {
		return nil, nil
	}

	return slm[name].Policy, nil
}

// SLMDiff permit to check if 2 policy are the same
func (h *ElasticsearchHandlerImpl) SLMDiff(actualObject, expectedObject, originalObject *SnapshotLifecyclePolicySpec) (patchResult *patch.PatchResult, err error) {
	return computeDiff(actualObject, expectedObject, originalObject)
}
