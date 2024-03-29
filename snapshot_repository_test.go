package eshandler

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/jarcoal/httpmock"
	olivere "github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
)

var urlSnapshotRepository = fmt.Sprintf("%s/_snapshot/test", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestSnapshotRespositoryGet() {

	snapshotRepository := make(olivere.SnapshotGetRepositoryResponse)
	snapshotRepository["test"] = &olivere.SnapshotRepositoryMetaData{
		Type: "fs",
		Settings: map[string]interface{}{
			"location": "/snapshot",
		},
	}

	httpmock.RegisterResponder("GET", urlSnapshotRepository, func(req *http.Request) (*http.Response, error) {
		resp, err := httpmock.NewJsonResponse(200, snapshotRepository)
		if err != nil {
			panic(err)
		}
		SetHeaders(resp)
		return resp, nil
	})

	repo, err := t.esHandler.SnapshotRepositoryGet("test")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), snapshotRepository["test"], repo)

	// When error
	httpmock.RegisterResponder("GET", urlSnapshotRepository, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.SnapshotRepositoryGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestSnapshotRepositoryDelete() {

	httpmock.RegisterResponder("DELETE", urlSnapshotRepository, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.SnapshotRepositoryDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("DELETE", urlSnapshotRepository, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.SnapshotRepositoryDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestSnapshotRepositoryUpdate() {

	snapshotRepository := &olivere.SnapshotRepositoryMetaData{
		Type: "fs",
		Settings: map[string]interface{}{
			"location": "/snapshot",
		},
	}

	httpmock.RegisterResponder("PUT", urlSnapshotRepository, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.SnapshotRepositoryUpdate("test", snapshotRepository)
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlSnapshotRepository, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.SnapshotRepositoryUpdate("test", snapshotRepository)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestSnapshotRepositoryDiff() {
	var actual, expected, original *olivere.SnapshotRepositoryMetaData

	expected = &olivere.SnapshotRepositoryMetaData{
		Type: "fs",
		Settings: map[string]interface{}{
			"location": "/snapshot",
		},
	}

	// When SLM not exist yet
	actual = nil
	diff, err := t.esHandler.SnapshotRepositoryDiff(actual, expected, nil)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When policy is the same
	actual = &olivere.SnapshotRepositoryMetaData{
		Type: "fs",
		Settings: map[string]interface{}{
			"location": "/snapshot",
		},
	}
	diff, err = t.esHandler.SnapshotRepositoryDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When policy is not the same
	expected.Type = "s3"
	diff, err = t.esHandler.SnapshotRepositoryDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When elastic set default values
	actual = &olivere.SnapshotRepositoryMetaData{
		Type: "fs",
		Settings: map[string]interface{}{
			"location": "/snapshot",
			"default": "plop",
		},
	}
	expected = &olivere.SnapshotRepositoryMetaData{
		Type: "fs",
		Settings: map[string]interface{}{
			"location": "/snapshot",
		},
	}
	original = &olivere.SnapshotRepositoryMetaData{
		Type: "fs",
		Settings: map[string]interface{}{
			"location": "/snapshot",
		},
	}

	diff, err = t.esHandler.SnapshotRepositoryDiff(actual, expected, original)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), actual, diff.Patched)

}
