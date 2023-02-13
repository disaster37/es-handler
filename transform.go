package eshandler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/disaster37/es-handler/v8/patch"
	jsonIterator "github.com/json-iterator/go"
	"github.com/pkg/errors"
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

	data, err := json.Marshal(transform)
	if err != nil {
		return err
	}

	res, err := h.client.API.TransformPutTransform(
		bytes.NewReader(data),
		name,
		h.client.API.TransformPutTransform.WithContext(context.Background()),
		h.client.API.TransformPutTransform.WithPretty(),
	)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		return errors.Errorf("Error when add transform %s: %s", name, res.String())
	}

	return nil

}

// TransformDelete permit to delete transform
func (h *ElasticsearchHandlerImpl) TransformDelete(name string) (err error) {

	res, err := h.client.API.TransformDeleteTransform(
		name,
		h.client.API.TransformDeleteTransform.WithContext(context.Background()),
		h.client.API.TransformDeleteTransform.WithPretty(),
	)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return nil
		}
		return errors.Errorf("Error when delete transform %s: %s", name, res.String())

	}

	return nil
}

// TransformGet permit to get transform
func (h *ElasticsearchHandlerImpl) TransformGet(name string) (transform *Transform, err error) {

	res, err := h.client.API.TransformGetTransform(
		h.client.API.TransformGetTransform.WithTransformID(name),
		h.client.API.TransformGetTransform.WithContext(context.Background()),
		h.client.API.TransformGetTransform.WithPretty(),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.IsError() {
		if res.StatusCode == 404 {
			return nil, nil
		}
		return nil, errors.Errorf("Error when get transform %s: %s", name, res.String())

	}
	b, err := io.ReadAll(res.Body)
	if err != nil {
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
	// If not yet exist
	if actualObject == nil {
		expected, err := jsonIterator.ConfigCompatibleWithStandardLibrary.Marshal(expectedObject)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to convert expected object to byte sequence")
		}

		return &patch.PatchResult{
			Patch:    expected,
			Current:  expected,
			Modified: expected,
			Original: nil,
			Patched: expectedObject,
		}, nil
	}

	return patch.DefaultPatchMaker.Calculate(actualObject, expectedObject, originalObject)
}
