package eshandler

import (
	"errors"
	"fmt"
	"net/http"

	elasticsearch "github.com/disaster37/elasticsearch/v9"
	esapi "github.com/disaster37/elasticsearch/v9/api"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

var urlRoleMapping = fmt.Sprintf("%s/_security/role_mapping/test", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestRoleMappingGet() {

	result := make(map[string]*esapi.SecurityRoleMapping)
	roleMapping := &esapi.SecurityRoleMapping{
		Enabled: true,
		Roles:   []string{"superuser"},
		Rules: map[string]any{
			"field": map[string]any{
				"groups": "cn=admins,dc=example,dc=com",
			},
		},
	}
	result["test"] = roleMapping

	httpmock.RegisterResponder("GET", urlRoleMapping, func(req *http.Request) (*http.Response, error) {
		resp, err := httpmock.NewJsonResponse(200, result)
		if err != nil {
			panic(err)
		}
		SetHeaders(resp)
		return resp, nil
	})

	resp, err := t.esHandler.RoleMappingGet("test")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), roleMapping, resp)

	// When not found
	httpmock.RegisterResponder("GET", urlRoleMapping, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	resp, err = t.esHandler.RoleMappingGet("test")
	assert.NoError(t.T(), err)
	assert.Nil(t.T(), resp)

	// When unauthorized
	httpmock.RegisterResponder("GET", urlRoleMapping, httpmock.NewStringResponder(401, `{"error":{"type":"security_exception","reason":"unauthorized"},"status":401}`))
	_, err = t.esHandler.RoleMappingGet("test")
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsUnauthorized(err))

	// When error
	httpmock.RegisterResponder("GET", urlRoleMapping, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.RoleMappingGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestRoleMappingDelete() {

	httpmock.RegisterResponder("DELETE", urlRoleMapping, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.RoleMappingDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When not found
	httpmock.RegisterResponder("DELETE", urlRoleMapping, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	err = t.esHandler.RoleMappingDelete("test")
	assert.NoError(t.T(), err)

	// When empty name
	err = t.esHandler.RoleMappingDelete("")
	assert.Error(t.T(), err)

	// When error
	httpmock.RegisterResponder("DELETE", urlRoleMapping, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.RoleMappingDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestRoleMappingUpdate() {
	roleMapping := &esapi.SecurityRoleMapping{
		Enabled: true,
		Roles:   []string{"superuser"},
		Rules: map[string]any{
			"field": map[string]any{
				"groups": "cn=admins,dc=example,dc=com",
			},
		},
	}

	httpmock.RegisterResponder("PUT", urlRoleMapping, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.RoleMappingUpdate("test", roleMapping)
	if err != nil {
		t.Fail(err.Error())
	}

	// When empty name
	err = t.esHandler.RoleMappingUpdate("", roleMapping)
	assert.Error(t.T(), err)

	// When conflict
	httpmock.RegisterResponder("PUT", urlRoleMapping, httpmock.NewStringResponder(409, `{"error":{"type":"version_conflict_engine_exception","reason":"conflict"},"status":409}`))
	err = t.esHandler.RoleMappingUpdate("test", roleMapping)
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsConflict(err))

	// When error
	httpmock.RegisterResponder("PUT", urlRoleMapping, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.RoleMappingUpdate("test", roleMapping)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestRoleMappingDiff() {
	var actual, expected, original *esapi.SecurityRoleMapping

	expected = &esapi.SecurityRoleMapping{
		Enabled: true,
		Roles:   []string{"superuser"},
		Rules: map[string]any{
			"field": map[string]any{
				"groups": "cn=admins,dc=example,dc=com",
			},
		},
	}

	// When role mapping not exist yet
	actual = nil
	diff, err := t.esHandler.RoleMappingDiff(actual, expected, nil)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When role mapping is the same
	actual = &esapi.SecurityRoleMapping{
		Enabled: true,
		Roles:   []string{"superuser"},
		Rules: map[string]any{
			"field": map[string]any{
				"groups": "cn=admins,dc=example,dc=com",
			},
		},
	}
	diff, err = t.esHandler.RoleMappingDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When role mapping is not the same
	expected.Roles = []string{"kibana_reader"}
	diff, err = t.esHandler.RoleMappingDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When Elastic add default value
	actual = &esapi.SecurityRoleMapping{
		Enabled: true,
		Roles:   []string{"superuser"},
		Rules: map[string]any{
			"field": map[string]any{
				"groups": "cn=admins,dc=example,dc=com",
			},
		},
		Metadata: map[string]any{
			"default": "test",
		},
	}

	expected = &esapi.SecurityRoleMapping{
		Enabled: true,
		Roles:   []string{"superuser"},
		Rules: map[string]any{
			"field": map[string]any{
				"groups": "cn=admins,dc=example,dc=com",
			},
		},
	}

	original = &esapi.SecurityRoleMapping{
		Enabled: true,
		Roles:   []string{"superuser"},
		Rules: map[string]any{
			"field": map[string]any{
				"groups": "cn=admins,dc=example,dc=com",
			},
		},
	}

	diff, err = t.esHandler.RoleMappingDiff(actual, expected, original)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), actual, diff.Patched)

}
