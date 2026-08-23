package eshandler

import (
	"context"
	"encoding/json"
	"net/url"

	elasticsearch "github.com/disaster37/elasticsearch/v9"
	"github.com/disaster37/generic-objectmatcher/patch"
	"github.com/pkg/errors"
)

// Some fix not provided by olivere
type XPackSecurityRole struct {
	RunAs             []string                             `json:"run_as,omitempty"`
	Cluster           []string                             `json:"cluster,omitempty"`
	Indices           []XPackSecurityIndicesPermissions    `json:"indices,omitempty"`
	Applications      []XPackSecurityApplicationPrivileges `json:"applications,omitempty"`
	Global            map[string]interface{}               `json:"global,omitempty"`
	Metadata          map[string]interface{}               `json:"metadata,omitempty"`
	TransientMetadata map[string]interface{}               `json:"transient_metadata,omitempty"`
}

// XPackSecurityApplicationPrivileges is the application privileges object
type XPackSecurityApplicationPrivileges struct {
	Application string   `json:"application"`
	Privileges  []string `json:"privileges,omitempty"`
	Resources   []string `json:"resources,omitempty"`
}

// XPackSecurityIndicesPermissions is the indices permission object
type XPackSecurityIndicesPermissions struct {
	Names                  []string    `json:"names"`
	Privileges             []string    `json:"privileges"`
	FieldSecurity          interface{} `json:"field_security,omitempty"`
	Query                  string      `json:"query,omitempty"`
	AllowRestrictedIndices bool        `json:"allow_restricted_indices,omitempty"`
}

// RoleUpdate permit to update role
func (h *ElasticsearchHandlerImpl) RoleUpdate(name string, role *XPackSecurityRole) (err error) {
	_, err = h.client.Security().PutRole(context.Background(), name, role)
	if err != nil {
		return errors.Wrapf(err, "Error when add role %s", name)
	}

	return nil
}

// RoleDelete permit to delete role
func (h *ElasticsearchHandlerImpl) RoleDelete(name string) (err error) {
	_, err = h.client.Security().DeleteRole(context.Background(), name)
	if err != nil {
		if elasticsearch.IsNotFound(err) {
			return nil
		}
		return err
	}

	h.log.Infof("Deleted role %s successfully", name)

	return nil
}

// RoleGet permit to get role
func (h *ElasticsearchHandlerImpl) RoleGet(name string) (role *XPackSecurityRole, err error) {
	b, err := h.getRaw("/_security/role/" + url.PathEscape(name))
	if err != nil || b == nil {
		return nil, err
	}

	h.log.Debugf("Get role %s successfully:\n%s", name, string(b))
	roleResp := make(map[string]XPackSecurityRole)
	if err = json.Unmarshal(b, &roleResp); err != nil {
		return nil, err
	}

	tmp := roleResp[name]

	return &tmp, nil
}

// RoleDiff permit to check if 2 role are the same
func (h *ElasticsearchHandlerImpl) RoleDiff(actualObject, expectedObject, originalObject *XPackSecurityRole) (patchResult *patch.PatchResult, err error) {
	return computeDiff(actualObject, expectedObject, originalObject)
}
