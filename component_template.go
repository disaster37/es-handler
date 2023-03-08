package eshandler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/disaster37/es-handler/v8/patch"
	jsonIterator "github.com/json-iterator/go"
	olivere "github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
)

// ComponentTemplateUpdate permit to update component template
func (h *ElasticsearchHandlerImpl) ComponentTemplateUpdate(name string, component *olivere.IndicesGetComponentTemplate) (err error) {

	data, err := json.Marshal(component)
	if err != nil {
		return err
	}

	res, err := h.client.API.Cluster.PutComponentTemplate(
		name,
		bytes.NewReader(data),
		h.client.API.Cluster.PutComponentTemplate.WithContext(context.Background()),
		h.client.API.Cluster.PutComponentTemplate.WithPretty(),
	)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		return errors.Errorf("Error when add index component template %s: %s", name, res.String())
	}

	return nil
}

// ComponentTemplateDelete permit to delete component template
func (h *ElasticsearchHandlerImpl) ComponentTemplateDelete(name string) (err error) {

	res, err := h.client.API.Cluster.DeleteComponentTemplate(
		name,
		h.client.API.Cluster.DeleteComponentTemplate.WithContext(context.Background()),
		h.client.API.Cluster.DeleteComponentTemplate.WithPretty(),
	)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return nil
		}
		return errors.Errorf("Error when delete index component template %s: %s", name, res.String())

	}

	return nil

}

// ComponentTemplateGet permit to get component template
func (h *ElasticsearchHandlerImpl) ComponentTemplateGet(name string) (component *olivere.IndicesGetComponentTemplate, err error) {

	res, err := h.client.API.Cluster.GetComponentTemplate(
		h.client.API.Cluster.GetComponentTemplate.WithName(name),
		h.client.API.Cluster.GetComponentTemplate.WithContext(context.Background()),
		h.client.API.Cluster.GetComponentTemplate.WithPretty(),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.IsError() {
		if res.StatusCode == 404 {
			return nil, nil
		}
		return nil, errors.Errorf("Error when get index component template %s: %s", name, res.String())

	}
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	indexComponentTemplateResp := &olivere.IndicesGetComponentTemplateResponse{}
	if err := json.Unmarshal(b, indexComponentTemplateResp); err != nil {
		return nil, err
	}

	if len(indexComponentTemplateResp.ComponentTemplates) == 0 {
		return nil, nil
	}

	return indexComponentTemplateResp.ComponentTemplates[0].ComponentTemplate, nil
}

// ComponentTemplateDiff permit to check if 2 component template are the same
func (h *ElasticsearchHandlerImpl) ComponentTemplateDiff(actualObject, expectedObject, originalObject *olivere.IndicesGetComponentTemplate) (patchResult *patch.PatchResult, err error) {
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
			Patched:  expectedObject,
		}, nil
	}

	return patch.DefaultPatchMaker.Calculate(actualObject, expectedObject, originalObject, patch.ConvertComponentTemplateSetting)
}
