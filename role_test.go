package eshandler

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

var urlRole = fmt.Sprintf("%s/_security/role/test", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestRoleGet() {

	result := make(map[string]XPackSecurityRole)
	role := &XPackSecurityRole{
		Cluster: []string{"all"},
		Indices: []XPackSecurityIndicesPermissions{
			{
				Names:      []string{"logstash-*"},
				Privileges: []string{"read"},
			},
		},
	}
	result["test"] = *role

	httpmock.RegisterResponder("GET", urlRole, func(req *http.Request) (*http.Response, error) {
		resp, err := httpmock.NewJsonResponse(200, result)
		if err != nil {
			panic(err)
		}
		SetHeaders(resp)
		return resp, nil
	})

	resp, err := t.esHandler.RoleGet("test")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), role, resp)

	// When error
	httpmock.RegisterResponder("GET", urlRole, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.RoleGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestRoleDelete() {

	httpmock.RegisterResponder("DELETE", urlRole, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.RoleDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("DELETE", urlRole, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.RoleDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestRoleUpdate() {
	role := &XPackSecurityRole{
		Cluster: []string{"all"},
		Indices: []XPackSecurityIndicesPermissions{
			{
				Names:      []string{"logstash-*"},
				Privileges: []string{"read"},
			},
		},
	}

	httpmock.RegisterResponder("PUT", urlRole, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.RoleUpdate("test", role)
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlRole, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.RoleUpdate("test", role)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestRoleDiff() {
	var actual, expected, original *XPackSecurityRole

	expected = &XPackSecurityRole{
		Cluster: []string{"all"},
		Indices: []XPackSecurityIndicesPermissions{
			{
				Names:      []string{"logstash-*"},
				Privileges: []string{"read"},
			},
		},
	}

	// When role not exist yet
	actual = nil
	diff, err := t.esHandler.RoleDiff(actual, expected, nil)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When role is the same
	actual = &XPackSecurityRole{
		Cluster: []string{"all"},
		Indices: []XPackSecurityIndicesPermissions{
			{
				Names:      []string{"logstash-*"},
				Privileges: []string{"read"},
			},
		},
	}
	diff, err = t.esHandler.RoleDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When role is not the same
	expected.Indices = []XPackSecurityIndicesPermissions{
		{
			Names:      []string{"test-*"},
			Privileges: []string{"read"},
		},
	}
	diff, err = t.esHandler.RoleDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When elastic add default values
	expected = &XPackSecurityRole{
		Cluster: []string{"all"},
		Indices: []XPackSecurityIndicesPermissions{
			{
				Names:      []string{"logstash-*"},
				Privileges: []string{"read"},
			},
		},
	}

	original = &XPackSecurityRole{
		Cluster: []string{"all"},
		Indices: []XPackSecurityIndicesPermissions{
			{
				Names:      []string{"logstash-*"},
				Privileges: []string{"read"},
			},
		},
	}

	actual = &XPackSecurityRole{
		Cluster: []string{"all"},
		Indices: []XPackSecurityIndicesPermissions{
			{
				Names:      []string{"logstash-*"},
				Privileges: []string{"read"},
			},
		},
		Metadata: map[string]interface{}{
			"default": "plop",
		},
	}

	diff, err = t.esHandler.RoleDiff(actual, expected, original)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), actual, diff.Patched)

}
