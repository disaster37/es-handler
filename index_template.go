package eshandler

import (
	"context"
	"encoding/json"
	"net/url"

	esapi "github.com/disaster37/elasticsearch/v9/api"
	localpatch "github.com/disaster37/es-handler/v9/patch"
	"github.com/disaster37/generic-objectmatcher/patch"
)

// IndexTemplateUpdate permit to create or update index template
func (h *ElasticsearchHandlerImpl) IndexTemplateUpdate(name string, template *localpatch.IndexTemplate) (err error) {
	req := &esapi.PutIndexTemplateRequest{Name: name, Body: template}
	_, err = h.client.Indices().PutIndexTemplate(context.Background(), req)
	return err
}

// IndexTemplateDelete permit to delete index template
func (h *ElasticsearchHandlerImpl) IndexTemplateDelete(name string) (err error) {
	_, err = h.client.Indices().DeleteIndexTemplate(context.Background(), name, nil)
	return ignoreNotFound(err)
}

// IndexTemplateGet permit to get index template
func (h *ElasticsearchHandlerImpl) IndexTemplateGet(name string) (template *localpatch.IndexTemplate, err error) {
	b, err := h.getRaw("/_index_template/" + url.PathEscape(name))
	if err != nil || b == nil {
		return nil, err
	}

	resp := &localpatch.IndexTemplateGetResponse{}
	if err := json.Unmarshal(b, resp); err != nil {
		return nil, err
	}

	if len(resp.IndexTemplates) == 0 {
		return nil, nil
	}

	return resp.IndexTemplates[0].IndexTemplate, nil
}

// IndexTemplateDiff permit to check if 2 index template is the same
func (h *ElasticsearchHandlerImpl) IndexTemplateDiff(actualObject, expectedObject, originalObject *localpatch.IndexTemplate) (patchResult *patch.PatchResult, err error) {
	return computeDiff(actualObject, expectedObject, originalObject, localpatch.ConvertIndexTemplateSetting)
}
