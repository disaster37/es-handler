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

var urlLicense = fmt.Sprintf("%s/_license", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestLicenseGet() {

	// Normale use case
	httpmock.RegisterResponder("GET", urlLicense, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{"license":{"uid":"test","type":"basic"}}`)
		SetHeaders(resp)
		return resp, nil
	})

	license, err := t.esHandler.LicenseGet()
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), "test", license.UID)
	assert.Equal(t.T(), "basic", license.Type)

	// When not found
	httpmock.RegisterResponder("GET", urlLicense, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	license, err = t.esHandler.LicenseGet()
	assert.NoError(t.T(), err)
	assert.Nil(t.T(), license)

	// When unauthorized
	httpmock.RegisterResponder("GET", urlLicense, httpmock.NewStringResponder(401, `{"error":{"type":"security_exception","reason":"unauthorized"},"status":401}`))
	_, err = t.esHandler.LicenseGet()
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsUnauthorized(err))

	// When error
	httpmock.RegisterResponder("GET", urlLicense, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.LicenseGet()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestLicenseDelete() {

	// Normale use case
	httpmock.RegisterResponder("DELETE", urlLicense, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.LicenseDelete()
	if err != nil {
		t.Fail(err.Error())
	}

	// When not found
	httpmock.RegisterResponder("DELETE", urlLicense, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	err = t.esHandler.LicenseDelete()
	assert.NoError(t.T(), err)

	// When error
	httpmock.RegisterResponder("DELETE", urlLicense, httpmock.NewErrorResponder(errors.New("Fake error")))
	err = t.esHandler.LicenseDelete()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestLicenseUpdate() {

	license := `{"license":{"uid":"test","type":"basic"}}`

	// Normale use case
	httpmock.RegisterResponder("PUT", urlLicense, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.Equal(t.T(), license, string(b))
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.LicenseUpdate(license)
	if err != nil {
		t.Fail(err.Error())
	}

	// When conflict
	httpmock.RegisterResponder("PUT", urlLicense, httpmock.NewStringResponder(409, `{"error":{"type":"version_conflict_engine_exception","reason":"conflict"},"status":409}`))
	err = t.esHandler.LicenseUpdate(license)
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsConflict(err))

	// When error
	httpmock.RegisterResponder("PUT", urlLicense, httpmock.NewErrorResponder(errors.New("Fake error")))
	err = t.esHandler.LicenseUpdate(license)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestLicenseEnableBasic() {

	// Normale use case
	httpmock.RegisterResponder("GET", fmt.Sprintf("%s/basic_status", urlLicense), func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{"eligible_to_start_basic": true}`)
		SetHeaders(resp)
		return resp, nil
	})
	httpmock.RegisterResponder("POST", fmt.Sprintf("%s/start_basic", urlLicense), func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.LicenseEnableBasic()
	if err != nil {
		t.Fail(err.Error())
	}

	// Not eligible
	httpmock.RegisterResponder("GET", fmt.Sprintf("%s/basic_status", urlLicense), func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{"eligible_to_start_basic": false}`)
		SetHeaders(resp)
		return resp, nil
	})
	httpmock.RegisterResponder("POST", fmt.Sprintf("%s/start_basic", urlLicense), func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})
	err = t.esHandler.LicenseEnableBasic()
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("GET", fmt.Sprintf("%s/basic_status", urlLicense), func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, `{"eligible_to_start_basic": true}`)
		SetHeaders(resp)
		return resp, nil
	})
	httpmock.RegisterResponder("POST", fmt.Sprintf("%s/start_basic", urlLicense), httpmock.NewErrorResponder(errors.New("fake error")))
	err = t.esHandler.LicenseEnableBasic()
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestLicenseDiff() {

	// No diff, same UID and not basic
	actual := &esapi.LicenseInfo{
		UID:  "test",
		Type: "gold",
	}
	new := &esapi.LicenseInfo{
		UID:  "test",
		Type: "gold",
	}

	assert.False(t.T(), t.esHandler.LicenseDiff(actual, new))

	// No diff, basic license
	actual = &esapi.LicenseInfo{
		UID:  "test",
		Type: "basic",
	}
	new = &esapi.LicenseInfo{
		UID:  "test2",
		Type: "basic",
	}
	assert.False(t.T(), t.esHandler.LicenseDiff(actual, new))

	// Diff, not same id and not basic
	actual = &esapi.LicenseInfo{
		UID:  "test",
		Type: "gold",
	}
	new = &esapi.LicenseInfo{
		UID:  "test2",
		Type: "gold",
	}
	assert.True(t.T(), t.esHandler.LicenseDiff(actual, new))

	// Diff, not same license type
	actual = &esapi.LicenseInfo{
		UID:  "test",
		Type: "gold",
	}
	new = &esapi.LicenseInfo{
		UID:  "test2",
		Type: "basic",
	}
	assert.True(t.T(), t.esHandler.LicenseDiff(actual, new))

}
