package eshandler

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	elasticsearch "github.com/disaster37/elasticsearch/v9"
	esapi "github.com/disaster37/elasticsearch/v9/api"
	"github.com/google/go-cmp/cmp"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

var urlILM = fmt.Sprintf("%s/_ilm/policy/test", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestILMGet() {

	rawPolicy := `
{
	"test" : {
		"policy": {
			"phases": {
				"warm": {
					"min_age": "10d",
					"actions": {
						"forcemerge": {
							"max_num_segments": 1
						}
					}
				},
				"delete": {
					"min_age": "31d",
					"actions": {
						"delete": {
							"delete_searchable_snapshot": true
						}
					}
				}
			}
		}
	}
}
	`

	policyTest := map[string]*esapi.IlmPolicy{}
	if err := json.Unmarshal([]byte(rawPolicy), &policyTest); err != nil {
		panic(err)
	}

	httpmock.RegisterResponder("GET", urlILM, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, rawPolicy)
		SetHeaders(resp)
		return resp, nil
	})

	policy, err := t.esHandler.ILMGet("test")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Empty(t.T(), cmp.Diff(policyTest["test"], policy))

	// When not found
	httpmock.RegisterResponder("GET", urlILM, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	policy, err = t.esHandler.ILMGet("test")
	assert.NoError(t.T(), err)
	assert.Nil(t.T(), policy)

	// When error
	httpmock.RegisterResponder("GET", urlILM, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.ILMGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestILMDelete() {

	httpmock.RegisterResponder("DELETE", urlILM, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.ILMDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When not found
	httpmock.RegisterResponder("DELETE", urlILM, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	err = t.esHandler.ILMDelete("test")
	assert.NoError(t.T(), err)

	// When empty name
	err = t.esHandler.ILMDelete("")
	assert.Error(t.T(), err)

	// When error
	httpmock.RegisterResponder("DELETE", urlILM, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.ILMDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestILMUpdate() {

	rawPolicy := `
{
	"policy": {
		"phases": {
			"warm": {
				"min_age": "10d",
				"actions": {
					"forcemerge": {
						"max_num_segments": 1
					}
				}
			},
			"delete": {
				"min_age": "31d",
				"actions": {
					"delete": {
						"delete_searchable_snapshot": true
					}
				}
			}
		}
	}
}
	`

	policy := &esapi.IlmPolicy{}
	if err := json.Unmarshal([]byte(rawPolicy), policy); err != nil {
		panic(err)
	}

	httpmock.RegisterResponder("PUT", urlILM, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), rawPolicy, string(b))
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.ILMUpdate("test", policy)
	if err != nil {
		t.Fail(err.Error())
	}

	// When empty name
	err = t.esHandler.ILMUpdate("", policy)
	assert.Error(t.T(), err)

	// When conflict
	httpmock.RegisterResponder("PUT", urlILM, httpmock.NewStringResponder(409, `{"error":{"type":"version_conflict_engine_exception","reason":"conflict"},"status":409}`))
	err = t.esHandler.ILMUpdate("test", policy)
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsConflict(err))

	// When error
	httpmock.RegisterResponder("PUT", urlILM, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.ILMUpdate("test", policy)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestILMDiff() {
	var actual, expected, original *esapi.IlmPolicy

	rawPolicy := `
{
	"policy": {
		"phases": {
			"warm": {
				"min_age": "10d",
				"actions": {
					"forcemerge": {
						"max_num_segments": 1
					}
				}
			},
			"delete": {
				"min_age": "31d",
				"actions": {
					"delete": {}
				}
			}
		}
	}
}
	`

	expected = &esapi.IlmPolicy{}
	if err := json.Unmarshal([]byte(rawPolicy), expected); err != nil {
		panic(err)
	}

	// When ILM not exist yet
	actual = nil
	diff, err := t.esHandler.ILMDiff(actual, expected, nil)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When policy is the same
	actual = &esapi.IlmPolicy{}
	if err := json.Unmarshal([]byte(rawPolicy), &actual); err != nil {
		panic(err)
	}
	diff, err = t.esHandler.ILMDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When policy is not the same
	rawPolicy = `
{
	"policy": {
		"phases": {
			"warm": {
				"min_age": "20d",
				"actions": {
					"forcemerge": {
						"max_num_segments": 1
					}
				}
			},
			"delete": {
				"min_age": "20d",
				"actions": {
					"delete": {
					}
				}
			}
		}
	}
}
	`
	expected = &esapi.IlmPolicy{}
	if err := json.Unmarshal([]byte(rawPolicy), expected); err != nil {
		panic(err)
	}
	diff, err = t.esHandler.ILMDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When elastic add default values
	rawPolicy = `
{
	"policy": {
		"phases": {
			"warm": {
				"min_age": "20d",
				"actions": {
					"forcemerge": {
						"max_num_segments": 1
					}
				}
			},
			"delete": {
				"min_age": "20d",
				"actions": {
					"delete": {
						"delete_searchable_snapshot": true
					}
				}
			}
		}
	}
}
	`
	actual = &esapi.IlmPolicy{}
	if err := json.Unmarshal([]byte(rawPolicy), actual); err != nil {
		panic(err)
	}
	rawPolicy = `
{
	"policy": {
		"phases": {
			"warm": {
				"min_age": "20d",
				"actions": {
					"forcemerge": {
						"max_num_segments": 1
					}
				}
			},
			"delete": {
				"min_age": "20d",
				"actions": {
					"delete": {
					}
				}
			}
		}
	}
}
	`
	expected = &esapi.IlmPolicy{}
	if err := json.Unmarshal([]byte(rawPolicy), expected); err != nil {
		panic(err)
	}

	original = &esapi.IlmPolicy{}
	if err := json.Unmarshal([]byte(rawPolicy), original); err != nil {
		panic(err)
	}

	diff, err = t.esHandler.ILMDiff(actual, expected, original)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), actual, diff.Patched)

}
