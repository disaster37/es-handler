package eshandler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/disaster37/generic-objectmatcher/patch"
	jsonIterator "github.com/json-iterator/go"
	olivere "github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	localpatch "github.com/disaster37/es-handler/v8/patch"
)

// IndexTemplateUpdate permit to create or update index template
func (h *ElasticsearchHandlerImpl) IndexTemplateUpdate(name string, template *olivere.IndicesGetIndexTemplate) (err error) {

	data, err := json.Marshal(template)
	if err != nil {
		return err
	}

	res, err := h.client.API.Indices.PutIndexTemplate(
		name,
		bytes.NewReader(data),
		h.client.API.Indices.PutIndexTemplate.WithContext(context.Background()),
		h.client.API.Indices.PutIndexTemplate.WithPretty(),
	)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		return errors.Errorf("Error when add index template %s: %s", name, res.String())
	}

	return nil

}

// IndexTemplateDelete permit to delete index template
func (h *ElasticsearchHandlerImpl) IndexTemplateDelete(name string) (err error) {

	res, err := h.client.API.Indices.DeleteIndexTemplate(
		name,
		h.client.API.Indices.DeleteIndexTemplate.WithContext(context.Background()),
		h.client.API.Indices.DeleteIndexTemplate.WithPretty(),
	)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return nil
		}
		return errors.Errorf("Error when delete index template %s: %s", name, res.String())

	}

	return nil
}

// IndexTemplateGet permit to get index template
func (h *ElasticsearchHandlerImpl) IndexTemplateGet(name string) (template *olivere.IndicesGetIndexTemplate, err error) {

	res, err := h.client.API.Indices.GetIndexTemplate(
		h.client.API.Indices.GetIndexTemplate.WithName(name),
		h.client.API.Indices.GetIndexTemplate.WithContext(context.Background()),
		h.client.API.Indices.GetIndexTemplate.WithPretty(),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.IsError() {
		if res.StatusCode == 404 {
			return nil, nil
		}
		return nil, errors.Errorf("Error when get index template %s: %s", name, res.String())

	}
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	indexTemplate := &olivere.IndicesGetIndexTemplateResponse{}
	if err := json.Unmarshal(b, indexTemplate); err != nil {
		return nil, err
	}

	if len(indexTemplate.IndexTemplates) == 0 {
		return nil, nil
	}

	return indexTemplate.IndexTemplates[0].IndexTemplate, nil
}

// IndexTemplateDiff permit to check if 2 index template is the same
func (h *ElasticsearchHandlerImpl) IndexTemplateDiff(actualObject, expectedObject, originalObject *olivere.IndicesGetIndexTemplate) (patchResult *patch.PatchResult, err error) {
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

	return patch.DefaultPatchMaker.Calculate(actualObject, expectedObject, originalObject, localpatch.ConvertIndexTemplateSetting)
}
