package eshandler

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	elasticsearch "github.com/disaster37/elasticsearch/v9"
	"github.com/google/go-cmp/cmp"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

var urlWatch = fmt.Sprintf("%s/_watcher/watch/test", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestWatchGet() {

	rawWatch := `
	{
		"trigger" : {
		  "schedule" : { "cron" : "0 0/1 * * * ?" }
		},
		"input" : {
		  "search" : {
			"request" : {
			  "indices" : [
				"logstash*"
			  ],
			  "body" : {
				"query" : {
				  "bool" : {
					"must" : {
					  "match": {
						 "response": 404
					  }
					},
					"filter" : {
					  "range": {
						"@timestamp": {
						  "from": "{{ctx.trigger.scheduled_time}}||-5m",
						  "to": "{{ctx.trigger.triggered_time}}"
						}
					  }
					}
				  }
				}
			  }
			}
		  }
		},
		"condition" : {
		  "compare" : { "ctx.payload.hits.total" : { "gt" : 0 }}
		},
		"actions" : {
		  "email_admin" : {
			"email" : {
			  "to" : "admin@domain.host.com",
			  "subject" : "404 recently encountered"
			}
		  }
		}
	}
	`

	rawResp := `
	{
		"found": true,
		"_id": "my_watch",
		"_seq_no": 0,
		"_primary_term": 1,
		"_version": 1,
		"status": { 
		  "version": 1,
		  "state": {
			"active": true,
			"timestamp": "2015-05-26T18:21:08.630Z"
		  },
		  "actions": {
			"test_index": {
			  "ack": {
				"timestamp": "2015-05-26T18:21:08.630Z",
				"state": "awaits_successful_execution"
			  }
			}
		  }
		},
		"watch": %s
	  }
	`

	watchTest := &XPackWatch{}
	if err := json.Unmarshal([]byte(rawWatch), watchTest); err != nil {
		panic(err)
	}

	httpmock.RegisterResponder("GET", urlWatch, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, fmt.Sprintf(rawResp, rawWatch))
		SetHeaders(resp)
		return resp, nil
	})

	watch, err := t.esHandler.WatchGet("test")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Empty(t.T(), cmp.Diff(watchTest, watch))

	// When not found
	httpmock.RegisterResponder("GET", urlWatch, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	watch, err = t.esHandler.WatchGet("test")
	assert.NoError(t.T(), err)
	assert.Nil(t.T(), watch)

	// When empty name
	_, err = t.esHandler.WatchGet("")
	assert.Error(t.T(), err)

	// When error
	httpmock.RegisterResponder("GET", urlWatch, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.WatchGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestWatchDelete() {

	httpmock.RegisterResponder("DELETE", urlWatch, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.WatchDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When not found
	httpmock.RegisterResponder("DELETE", urlWatch, httpmock.NewStringResponder(404, `{"error":{"type":"resource_not_found_exception","reason":"not found"},"status":404}`))
	err = t.esHandler.WatchDelete("test")
	assert.NoError(t.T(), err)

	// When empty name
	err = t.esHandler.WatchDelete("")
	assert.Error(t.T(), err)

	// When error
	httpmock.RegisterResponder("DELETE", urlWatch, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.WatchDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestWatchUpdate() {

	rawWatch := `
	{
		"trigger" : {
		  "schedule" : { "cron" : "0 0/1 * * * ?" }
		},
		"input" : {
		  "search" : {
			"request" : {
			  "indices" : [
				"logstash*"
			  ],
			  "body" : {
				"query" : {
				  "bool" : {
					"must" : {
					  "match": {
						 "response": 404
					  }
					},
					"filter" : {
					  "range": {
						"@timestamp": {
						  "from": "{{ctx.trigger.scheduled_time}}||-5m",
						  "to": "{{ctx.trigger.triggered_time}}"
						}
					  }
					}
				  }
				}
			  }
			}
		  }
		},
		"condition" : {
		  "compare" : { "ctx.payload.hits.total" : { "gt" : 0 }}
		},
		"actions" : {
		  "email_admin" : {
			"email" : {
			  "to" : "admin@domain.host.com",
			  "subject" : "404 recently encountered"
			}
		  }
		}
	}
	`

	watchTest := &XPackWatch{}
	if err := json.Unmarshal([]byte(rawWatch), watchTest); err != nil {
		panic(err)
	}

	httpmock.RegisterResponder("PUT", urlWatch, func(req *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(req.Body)
		assert.JSONEq(t.T(), rawWatch, string(b))
		resp := httpmock.NewStringResponse(200, "{}")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.WatchUpdate("test", watchTest)
	if err != nil {
		t.Fail(err.Error())
	}

	// When empty name
	err = t.esHandler.WatchUpdate("", watchTest)
	assert.Error(t.T(), err)

	// When conflict
	httpmock.RegisterResponder("PUT", urlWatch, httpmock.NewStringResponder(409, `{"error":{"type":"version_conflict_engine_exception","reason":"conflict"},"status":409}`))
	err = t.esHandler.WatchUpdate("test", watchTest)
	assert.Error(t.T(), err)
	assert.True(t.T(), elasticsearch.IsConflict(err))

	// When error
	httpmock.RegisterResponder("PUT", urlWatch, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.WatchUpdate("test", watchTest)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestWatchDiff() {
	var actual, expected, original *XPackWatch
	rawWatch := `
	{
		"trigger" : {
		  "schedule" : { "cron" : "0 0/1 * * * ?" }
		},
		"input" : {
		  "search" : {
			"request" : {
			  "indices" : [
				"logstash*"
			  ],
			  "body" : {
				"query" : {
				  "bool" : {
					"must" : {
					  "match": {
						 "response": 404
					  }
					},
					"filter" : {
					  "range": {
						"@timestamp": {
						  "from": "{{ctx.trigger.scheduled_time}}||-5m",
						  "to": "{{ctx.trigger.triggered_time}}"
						}
					  }
					}
				  }
				}
			  }
			}
		  }
		},
		"condition" : {
		  "compare" : { "ctx.payload.hits.total" : { "gt" : 0 }}
		},
		"actions" : {
		  "email_admin" : {
			"email" : {
			  "to" : "admin@domain.host.com",
			  "subject" : "404 recently encountered"
			}
		  }
		}
	}
	`

	expected = &XPackWatch{}
	if err := json.Unmarshal([]byte(rawWatch), expected); err != nil {
		panic(err)
	}

	// When Watch not exist yet
	actual = nil
	diff, err := t.esHandler.WatchDiff(actual, expected, nil)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When watch is the same
	actual = &XPackWatch{}
	if err := json.Unmarshal([]byte(rawWatch), &actual); err != nil {
		panic(err)
	}
	diff, err = t.esHandler.WatchDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When watch is not the same
	rawWatch = `
	{
		"trigger" : {
		  "schedule" : { "cron" : "0 0/1 * * * ?" }
		},
		"input" : {
		  "search" : {
			"request" : {
			  "indices" : [
				"logstash*"
			  ],
			  "body" : {
				"query" : {
				  "bool" : {
					"must" : {
					  "match": {
						 "response": 404
					  }
					},
					"filter" : {
					  "range": {
						"@timestamp": {
						  "from": "{{ctx.trigger.scheduled_time}}||-5m",
						  "to": "{{ctx.trigger.triggered_time}}"
						}
					  }
					}
				  }
				}
			  }
			}
		  }
		},
		"condition" : {
		  "compare" : { "ctx.payload.hits.total" : { "gt" : 0 }}
		},
		"actions" : {
		  "email_admin" : {
			"email" : {
			  "to" : "admin@domain.host.com",
			  "subject" : "405 recently encountered"
			}
		  }
		}
	}
	`
	expected = &XPackWatch{}
	if err := json.Unmarshal([]byte(rawWatch), expected); err != nil {
		panic(err)
	}
	diff, err = t.esHandler.WatchDiff(actual, expected, actual)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.False(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), expected, diff.Patched)

	// When Elastic add default value
	rawWatch = `
	{
		"trigger" : {
		  "schedule" : { "cron" : "0 0/1 * * * ?" }
		},
		"input" : {
		  "search" : {
			"request" : {
			  "indices" : [
				"logstash*"
			  ],
			  "body" : {
				"query" : {
				  "bool" : {
					"must" : {
					  "match": {
						 "response": 404
					  }
					},
					"filter" : {
					  "range": {
						"@timestamp": {
						  "from": "{{ctx.trigger.scheduled_time}}||-5m",
						  "to": "{{ctx.trigger.triggered_time}}"
						}
					  }
					}
				  }
				}
			  }
			}
		  }
		},
		"condition" : {
		  "compare" : { "ctx.payload.hits.total" : { "gt" : 0 }}
		},
		"actions" : {
		  "email_admin" : {
			"email" : {
			  "to" : "admin@domain.host.com",
			  "subject" : "405 recently encountered"
			}
		  }
		},
		"metadata": {
			"default": "test"
		}
	}
	`
	actual = &XPackWatch{}
	if err := json.Unmarshal([]byte(rawWatch), actual); err != nil {
		panic(err)
	}

	rawWatch = `
	{
		"trigger" : {
		  "schedule" : { "cron" : "0 0/1 * * * ?" }
		},
		"input" : {
		  "search" : {
			"request" : {
			  "indices" : [
				"logstash*"
			  ],
			  "body" : {
				"query" : {
				  "bool" : {
					"must" : {
					  "match": {
						 "response": 404
					  }
					},
					"filter" : {
					  "range": {
						"@timestamp": {
						  "from": "{{ctx.trigger.scheduled_time}}||-5m",
						  "to": "{{ctx.trigger.triggered_time}}"
						}
					  }
					}
				  }
				}
			  }
			}
		  }
		},
		"condition" : {
		  "compare" : { "ctx.payload.hits.total" : { "gt" : 0 }}
		},
		"actions" : {
		  "email_admin" : {
			"email" : {
			  "to" : "admin@domain.host.com",
			  "subject" : "405 recently encountered"
			}
		  }
		}
	}
	`
	expected = &XPackWatch{}
	if err := json.Unmarshal([]byte(rawWatch), expected); err != nil {
		panic(err)
	}
	original = &XPackWatch{}
	if err := json.Unmarshal([]byte(rawWatch), original); err != nil {
		panic(err)
	}

	diff, err = t.esHandler.WatchDiff(actual, expected, original)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.True(t.T(), diff.IsEmpty())
	assert.Equal(t.T(), actual, diff.Patched)

}
