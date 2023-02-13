package eshandler

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/jarcoal/httpmock"
	olivere "github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
)

var urlRoleMapping = fmt.Sprintf("%s/_security/role_mapping/test", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestRoleMappingGet() {

	result := make(olivere.XPackSecurityGetRoleMappingResponse)
	roleMapping := &olivere.XPackSecurityRoleMapping{
		Enabled: true,
		Roles:   []string{"superuser"},
		Rules: map[string]any{
			"field": map[string]any{
				"groups": "cn=admins,dc=example,dc=com",
			},
		},
	}
	result["test"] = *roleMapping

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

	// When error
	httpmock.RegisterResponder("GET", urlRoleMapping, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.RoleMappingGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestRoleMappingDelete() {

	httpmock.RegisterResponder("DELETE", urlRoleMapping, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.RoleMappingDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("DELETE", urlRoleMapping, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.RoleMappingDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestRoleMappingUpdate() {
	roleMapping := &olivere.XPackSecurityRoleMapping{
		Enabled: true,
		Roles:   []string{"superuser"},
		Rules: map[string]any{
			"field": map[string]any{
				"groups": "cn=admins,dc=example,dc=com",
			},
		},
	}

	httpmock.RegisterResponder("PUT", urlRoleMapping, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.RoleMappingUpdate("test", roleMapping)
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlRoleMapping, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.RoleMappingUpdate("test", roleMapping)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestRoleMappingDiff() {
	var actual, expected, original *olivere.XPackSecurityRoleMapping

	expected = &olivere.XPackSecurityRoleMapping{
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
	actual = &olivere.XPackSecurityRoleMapping{
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
	actual = &olivere.XPackSecurityRoleMapping{
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

	expected = &olivere.XPackSecurityRoleMapping{
		Enabled: true,
		Roles:   []string{"superuser"},
		Rules: map[string]any{
			"field": map[string]any{
				"groups": "cn=admins,dc=example,dc=com",
			},
		},
	}

	original = &olivere.XPackSecurityRoleMapping{
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
