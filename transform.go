package eshandler

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/disaster37/generic-objectmatcher/patch"
)

type TransformGetResponse struct {
	Transforms []*Transform `json:"transforms"`
}

type Transform struct {
	Id          string              `json:"id,omitempty"`
	Version     string              `json:"version,omitempty"`
	CreateTime  int64               `json:"create_time,omitempty"`
	Description string              `json:"description,omitempty"`
	Destination *TransformDest      `json:"dest"`
	Frequency   string              `json:"frequency,omitempty"`
	Lastest     *TransformLatest    `json:"latest,omitempty"`
	Metadata    map[string]any      `json:"_meta,omitempty"`
	Pivot       *TransformPivot     `json:"pivot"`
	Retention   *TransformRetention `json:"retention_policy,omitempty"`
	Settings    map[string]any      `json:"settings,omitempty"`
	Source      *TransformSource    `json:"source"`
	Sync        *TransformSync      `json:"sync"`
}

type TransformLatest struct {
	Sort      string   `json:"sort"`
	UniqueKey []string `json:"unique_key"`
}

type TransformSource struct {
	Index           []string `json:"index"`
	Query           any      `json:"query,omitempty"`
	RuntimeMappings any      `json:"runtime_mappings,omitempty"`
}

type TransformDest struct {
	Index    string `json:"index"`
	Pipeline string `json:"pipeline,omitempty"`
}

type TransformSync struct {
	Time TransformSyncTime `json:"time"`
}

type TransformRetention struct {
	Time TransformRetentionTime `json:"time"`
}

type TransformSyncTime struct {
	Field string `json:"field"`
	Delay string `json:"delay,omitempty"`
}

type TransformRetentionTime struct {
	Field  string `json:"field"`
	MaxAge string `json:"max_age"`
}

type TransformPivot struct {
	GroupBy      map[string]any `json:"group_by"`
	Aggregations map[string]any `json:"aggregations"`
}

// TransformUpdate permit to create or update transform
func (h *ElasticsearchHandlerImpl) TransformUpdate(name string, transform *Transform) (err error) {
	_, err = h.client.Transform().Put(context.Background(), name, transform, nil)
	return err
}

// TransformDelete permit to delete transform
func (h *ElasticsearchHandlerImpl) TransformDelete(name string) (err error) {
	_, err = h.client.Transform().Delete(context.Background(), name, nil)
	return ignoreNotFound(err)
}

// TransformGet permit to get transform
func (h *ElasticsearchHandlerImpl) TransformGet(name string) (transform *Transform, err error) {
	b, err := h.getRaw("/_transform/" + url.PathEscape(name))
	if err != nil || b == nil {
		return nil, err
	}

	transforms := &TransformGetResponse{}
	if err := json.Unmarshal(b, transforms); err != nil {
		return nil, err
	}

	if len(transforms.Transforms) == 0 {
		return nil, nil
	}

	return transforms.Transforms[0], nil
}

// TransformDiff permit to check if 2 transform are the same
func (h *ElasticsearchHandlerImpl) TransformDiff(actualObject, expectedObject, originalObject *Transform) (patchResult *patch.PatchResult, err error) {
	return computeDiff(actualObject, expectedObject, originalObject)
}
