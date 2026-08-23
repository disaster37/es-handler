package eshandler

import (
	"context"

	elasticsearch "github.com/disaster37/elasticsearch/v9"
	esapi "github.com/disaster37/elasticsearch/v9/api"
	"github.com/disaster37/generic-objectmatcher/patch"
)

// RoleMappingUpdate permit to create or update role mapping
func (h *ElasticsearchHandlerImpl) RoleMappingUpdate(name string, roleMapping *esapi.SecurityRoleMapping) (err error) {
	_, err = h.client.Security().PutRoleMapping(context.Background(), name, roleMapping)
	return err
}

// RoleMappingDelete permit to delete role mapping
func (h *ElasticsearchHandlerImpl) RoleMappingDelete(name string) (err error) {
	_, err = h.client.Security().DeleteRoleMapping(context.Background(), name)
	if err != nil {
		if elasticsearch.IsNotFound(err) {
			return nil
		}
		return err
	}

	h.log.Infof("Deleted role mapping %s successfully", name)

	return nil
}

// RoleMappingGet permit to get role mapping
func (h *ElasticsearchHandlerImpl) RoleMappingGet(name string) (roleMapping *esapi.SecurityRoleMapping, err error) {
	mappings, err := h.client.Security().GetRoleMapping(context.Background(), []string{name})
	if err != nil {
		return nil, ignoreNotFound(err)
	}

	h.log.Infof("Read role mapping %s successfully", name)

	return mappings[name], nil
}

// RoleMappingDiff permit to check if 2 role mapping are the same
func (h *ElasticsearchHandlerImpl) RoleMappingDiff(actualObject, expectedObject, originalObject *esapi.SecurityRoleMapping) (patchResult *patch.PatchResult, err error) {
	return computeDiff(actualObject, expectedObject, originalObject)
}
