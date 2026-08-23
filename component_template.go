package eshandler

import (
	"context"
	"encoding/json"
	"net/url"

	esapi "github.com/disaster37/elasticsearch/v9/api"
	localpatch "github.com/disaster37/es-handler/v9/patch"
	"github.com/disaster37/generic-objectmatcher/patch"
)

// ComponentTemplateUpdate permit to update component template
func (h *ElasticsearchHandlerImpl) ComponentTemplateUpdate(name string, component *localpatch.ComponentTemplate) (err error) {
	req := &esapi.PutComponentTemplateRequest{Name: name, Body: component}
	_, err = h.client.Cluster().PutComponentTemplate(context.Background(), req)
	return err
}

// ComponentTemplateDelete permit to delete component template
func (h *ElasticsearchHandlerImpl) ComponentTemplateDelete(name string) (err error) {
	_, err = h.client.Cluster().DeleteComponentTemplate(context.Background(), name, nil)
	return ignoreNotFound(err)
}

// ComponentTemplateGet permit to get component template
func (h *ElasticsearchHandlerImpl) ComponentTemplateGet(name string) (component *localpatch.ComponentTemplate, err error) {
	b, err := h.getRaw("/_component_template/" + url.PathEscape(name))
	if err != nil || b == nil {
		return nil, err
	}

	resp := &localpatch.ComponentTemplateGetResponse{}
	if err := json.Unmarshal(b, resp); err != nil {
		return nil, err
	}

	if len(resp.ComponentTemplates) == 0 {
		return nil, nil
	}

	return resp.ComponentTemplates[0].ComponentTemplate, nil
}

// ComponentTemplateDiff permit to check if 2 component template are the same
func (h *ElasticsearchHandlerImpl) ComponentTemplateDiff(actualObject, expectedObject, originalObject *localpatch.ComponentTemplate) (patchResult *patch.PatchResult, err error) {
	return computeDiff(actualObject, expectedObject, originalObject, localpatch.ConvertComponentTemplateSetting)
}
