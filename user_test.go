package eshandler

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/jarcoal/httpmock"
	olivere "github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
)

var urlUser = fmt.Sprintf("%s/_security/user/test", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestUserGet() {

	result := make(olivere.XPackSecurityGetUserResponse)
	user := &olivere.XPackSecurityUser{
		Username: "test",
		Enabled:  true,
		Email:    "no@no.no",
		Fullname: "test",
		Roles:    []string{"kibana_user"},
	}
	result["test"] = *user

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

	// When error
	httpmock.RegisterResponder("GET", urlUser, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.UserGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestUserDelete() {

	httpmock.RegisterResponder("DELETE", urlUser, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.UserDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("DELETE", urlUser, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.UserDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestUserCreate() {
	user := &olivere.XPackSecurityPutUserRequest{
		Enabled:  true,
		Email:    "no@no.no",
		Roles:    []string{"kibana_user"},
		FullName: "test",
		Password: "password",
	}

	httpmock.RegisterResponder("PUT", urlUser, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.UserCreate("test", user)
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlUser, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.UserCreate("test", user)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestUserUpdate() {

	// When no should to change password
	user := &olivere.XPackSecurityPutUserRequest{
		Enabled:  true,
		Email:    "no@no.no",
		Roles:    []string{"kibana_user"},
		FullName: "test",
	}

	httpmock.RegisterResponder("PUT", urlUser, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.UserUpdate("test", user)
	if err != nil {
		t.Fail(err.Error())
	}

	// When should to change password
	user = &olivere.XPackSecurityPutUserRequest{
		Enabled:  true,
		Email:    "no@no.no",
		Roles:    []string{"kibana_user"},
		FullName: "test",
		Password: "password",
	}

	httpmock.RegisterResponder("PUT", urlUser, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})
	httpmock.RegisterResponder("PUT", fmt.Sprintf("%s/_password", urlUser), func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
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
	var actual, expected, original *olivere.XPackSecurityPutUserRequest

	expected = &olivere.XPackSecurityPutUserRequest{
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
	actual = &olivere.XPackSecurityPutUserRequest{
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
	actual = &olivere.XPackSecurityPutUserRequest{
		Enabled:  true,
		Email:    "no@no.no",
		Roles:    []string{"kibana_user"},
		FullName: "test",
		Password: "password",
		Metadata: map[string]interface{}{
			"default": "test",
		},
	}

	expected = &olivere.XPackSecurityPutUserRequest{
		Enabled:  true,
		Email:    "no@no.no",
		Roles:    []string{"kibana_user"},
		FullName: "test",
		Password: "password",
	}

	original = &olivere.XPackSecurityPutUserRequest{
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
