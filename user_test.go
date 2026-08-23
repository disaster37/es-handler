package eshandler

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	elasticsearch "github.com/disaster37/elasticsearch/v9"
	esapi "github.com/disaster37/elasticsearch/v9/api"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

var urlUser = fmt.Sprintf("%s/_security/user/test", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestUserGet() {

	result := make(map[string]*esapi.SecurityUser)
	user := &esapi.SecurityUser{
		Username: "test",
		Enabled:  true,
		Email:    "no@no.no",
		FullName: "test",
		Roles:    []string{"kibana_user"},
	}
	result["test"] = user

	httpmock.RegisterResponder("GET", urlUser, func(req *http.Request) (*http.Response, error) {
		resp, err := httpmock.NewJsonResponse(200, result)
		if err != nil {
			panic(err)
		}
		SetHeaders(resp)
		return resp, nil
	})

	resp, err := t.esHandler.UserGet("test")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), user, resp)

	// When not found
	httpmock.RegisterResponder("GET", urlUser, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	resp, err = t.esHandler.UserGet("test")
	assert.NoError(t.T(), err)
	assert.Nil(t.T(), resp)

	// When unauthorized
	httpmock.RegisterResponder("GET", urlUser, httpmock.NewStringResponder(401, `{"error":{"type":"security_exception","reason":"unauthorized"},"status":401}`))
	_, err = t.esHandler.UserGet("test")
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsUnauthorized(err))

	// When error
	httpmock.RegisterResponder("GET", urlUser, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.UserGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestUserDelete() {

	httpmock.RegisterResponder("DELETE", urlUser, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.UserDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When not found
	httpmock.RegisterResponder("DELETE", urlUser, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	err = t.esHandler.UserDelete("test")
	assert.NoError(t.T(), err)

	// When empty name
	err = t.esHandler.UserDelete("")
	assert.Error(t.T(), err)

	// When error
	httpmock.RegisterResponder("DELETE", urlUser, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.UserDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestUserCreate() {
	user := &SecurityPutUserRequest{
		Enabled:  true,
		Email:    "no@no.no",
		Roles:    []string{"kibana_user"},
		FullName: "test",
		Password: "password",
	}

	httpmock.RegisterResponder("PUT", urlUser, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), `{"enabled":true,"email":"no@no.no","full_name":"test","password":"password","roles":["kibana_user"]}`, string(b))
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.UserCreate("test", user)
	if err != nil {
		t.Fail(err.Error())
	}

	// When empty name
	err = t.esHandler.UserCreate("", user)
	assert.Error(t.T(), err)

	// When conflict
	httpmock.RegisterResponder("PUT", urlUser, httpmock.NewStringResponder(409, `{"error":{"type":"version_conflict_engine_exception","reason":"conflict"},"status":409}`))
	err = t.esHandler.UserCreate("test", user)
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsConflict(err))

	// When error
	httpmock.RegisterResponder("PUT", urlUser, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.UserCreate("test", user)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestUserUpdate() {

	// When no should to change password
	user := &SecurityPutUserRequest{
		Enabled:  true,
		Email:    "no@no.no",
		Roles:    []string{"kibana_user"},
		FullName: "test",
	}

	httpmock.RegisterResponder("PUT", urlUser, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.UserUpdate("test", user)
	if err != nil {
		t.Fail(err.Error())
	}

	// When should to change password
	user = &SecurityPutUserRequest{
		Enabled:  true,
		Email:    "no@no.no",
		Roles:    []string{"kibana_user"},
		FullName: "test",
		Password: "password",
	}

	httpmock.RegisterResponder("PUT", urlUser, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})
	httpmock.RegisterResponder("POST", fmt.Sprintf("%s/_password", urlUser), func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err = t.esHandler.UserUpdate("test", user)
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlUser, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.UserUpdate("test", user)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestUserDiff() {
	var actual, expected, original *SecurityPutUserRequest

	expected = &SecurityPutUserRequest{
		Enabled:  true,
		Email:    "no@no.no",
		Roles:    []string{"kibana_user"},
		FullName: "test",
		Password: "password",
	}

	// When user not exist yet
	actual = nil
	diff, err := t.esHandler.UserDiff(actual, expected, nil)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When user is the same
	actual = &SecurityPutUserRequest{
		Enabled:  true,
		Email:    "no@no.no",
		Roles:    []string{"kibana_user"},
		FullName: "test",
		Password: "password",
	}
	diff, err = t.esHandler.UserDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When user is not the same
	expected.Email = "no2@no.no"
	diff, err = t.esHandler.UserDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When Elastic add default value
	actual = &SecurityPutUserRequest{
		Enabled:  true,
		Email:    "no@no.no",
		Roles:    []string{"kibana_user"},
		FullName: "test",
		Password: "password",
		Metadata: map[string]interface{}{
			"default": "test",
		},
	}

	expected = &SecurityPutUserRequest{
		Enabled:  true,
		Email:    "no@no.no",
		Roles:    []string{"kibana_user"},
		FullName: "test",
		Password: "password",
	}

	original = &SecurityPutUserRequest{
		Enabled:  true,
		Email:    "no@no.no",
		Roles:    []string{"kibana_user"},
		FullName: "test",
		Password: "password",
	}

	diff, err = t.esHandler.UserDiff(actual, expected, original)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), actual, diff.Patched)

}
