package eshandler

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

var urlTransform = fmt.Sprintf("%s/_transform/test", baseURL)

func (t *ElasticsearchHandlerTestSuite) TestTransformGet() {

	result := &TransformGetResponse{}
	transform := &Transform{
		Source: &TransformSource{
			Index: []string{"kibana_sample_data_ecommerce"},
			Query: map[string]any {
				"term": map[string]any {
					"geoip.continent_name": map[string]any{
						"value": "Asia",
					},
				},
			},
		},
		Pivot: &TransformPivot{
			GroupBy: map[string]any {
				"customer_id": map[string]any{
					"terms": map[string]any{
						"field": "customer_id",
					},
				},
			},
			Aggregations: map[string]any{
				"max_price": map[string]any{
					"max": map[string]any{
						"field": "taxful_total_price",
					},
				},
			},
		},
		Description: "Maximum priced ecommerce data by customer_id in Asia",
		Destination: &TransformDest{
			Index: "kibana_sample_data_ecommerce_transform1",
			Pipeline: "add_timestamp_pipeline",
		},
		Frequency: "5m",
		Sync: &TransformSync{
			Time: TransformSyncTime{
				Field: "order_date",
				Delay: "60s",
			},
		},
		Retention: &TransformRetention{
			Time: TransformRetentionTime{
				Field: "order_date",
				MaxAge: "30d",
			},
		},
	}
	result.Transforms = []*Transform{transform}

	httpmock.RegisterResponder("GET", urlTransform, func(req *http.Request) (*http.Response, error) {
		resp, err := httpmock.NewJsonResponse(200, result)
		if err != nil {
			panic(err)
		}
		SetHeaders(resp)
		return resp, nil
	})

	resp, err := t.esHandler.TransformGet("test")
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Equal(t.T(), transform, resp)

	// When error
	httpmock.RegisterResponder("GET", urlTransform, httpmock.NewErrorResponder(errors.New("fack error")))
	_, err = t.esHandler.TransformGet("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestTransformDelete() {

	httpmock.RegisterResponder("DELETE", urlTransform, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.TransformDelete("test")
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("DELETE", urlTransform, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.TransformDelete("test")
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestTransformUpdate() {
	transform := &Transform{
		Source: &TransformSource{
			Index: []string{"kibana_sample_data_ecommerce"},
			Query: map[string]any {
				"term": map[string]any {
					"geoip.continent_name": map[string]any{
						"value": "Asia",
					},
				},
			},
		},
		Pivot: &TransformPivot{
			GroupBy: map[string]any {
				"customer_id": map[string]any{
					"terms": map[string]any{
						"field": "customer_id",
					},
				},
			},
			Aggregations: map[string]any{
				"max_price": map[string]any{
					"max": map[string]any{
						"field": "taxful_total_price",
					},
				},
			},
		},
		Description: "Maximum priced ecommerce data by customer_id in Asia",
		Destination: &TransformDest{
			Index: "kibana_sample_data_ecommerce_transform1",
			Pipeline: "add_timestamp_pipeline",
		},
		Frequency: "5m",
		Sync: &TransformSync{
			Time: TransformSyncTime{
				Field: "order_date",
				Delay: "60s",
			},
		},
		Retention: &TransformRetention{
			Time: TransformRetentionTime{
				Field: "order_date",
				MaxAge: "30d",
			},
		},
	}

	httpmock.RegisterResponder("PUT", urlTransform, func(req *http.Request) (*http.Response, error) {
		resp := httpmock.NewStringResponse(200, "")
		SetHeaders(resp)
		return resp, nil
	})

	err := t.esHandler.TransformUpdate("test", transform)
	if err != nil {
		t.Fail(err.Error())
	}

	// When error
	httpmock.RegisterResponder("PUT", urlTransform, httpmock.NewErrorResponder(errors.New("fack error")))
	err = t.esHandler.TransformUpdate("test", transform)
	assert.Error(t.T(), err)
}

func (t *ElasticsearchHandlerTestSuite) TestTransformDiff() {
	var actual, expected *Transform

	expected = &Transform{
		Source: &TransformSource{
			Index: []string{"kibana_sample_data_ecommerce"},
			Query: map[string]any {
				"term": map[string]any {
					"geoip.continent_name": map[string]any{
						"value": "Asia",
					},
				},
			},
		},
		Pivot: &TransformPivot{
			GroupBy: map[string]any {
				"customer_id": map[string]any{
					"terms": map[string]any{
						"field": "customer_id",
					},
				},
			},
			Aggregations: map[string]any{
				"max_price": map[string]any{
					"max": map[string]any{
						"field": "taxful_total_price",
					},
				},
			},
		},
		Description: "Maximum priced ecommerce data by customer_id in Asia",
		Destination: &TransformDest{
			Index: "kibana_sample_data_ecommerce_transform1",
			Pipeline: "add_timestamp_pipeline",
		},
		Frequency: "5m",
		Sync: &TransformSync{
			Time: TransformSyncTime{
				Field: "order_date",
				Delay: "60s",
			},
		},
		Retention: &TransformRetention{
			Time: TransformRetentionTime{
				Field: "order_date",
				MaxAge: "30d",
			},
		},
	}

	// When transform not exist yet
	actual = nil
	diff, err := t.esHandler.TransformDiff(actual, expected)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.NotEmpty(t.T(), diff)

	// When transform is the same
	actual = &Transform{
		Source: &TransformSource{
			Index: []string{"kibana_sample_data_ecommerce"},
			Query: map[string]any {
				"term": map[string]any {
					"geoip.continent_name": map[string]any{
						"value": "Asia",
					},
				},
			},
		},
		Pivot: &TransformPivot{
			GroupBy: map[string]any {
				"customer_id": map[string]any{
					"terms": map[string]any{
						"field": "customer_id",
					},
				},
			},
			Aggregations: map[string]any{
				"max_price": map[string]any{
					"max": map[string]any{
						"field": "taxful_total_price",
					},
				},
			},
		},
		Description: "Maximum priced ecommerce data by customer_id in Asia",
		Destination: &TransformDest{
			Index: "kibana_sample_data_ecommerce_transform1",
			Pipeline: "add_timestamp_pipeline",
		},
		Frequency: "5m",
		Sync: &TransformSync{
			Time: TransformSyncTime{
				Field: "order_date",
				Delay: "60s",
			},
		},
		Retention: &TransformRetention{
			Time: TransformRetentionTime{
				Field: "order_date",
				MaxAge: "30d",
			},
		},
	}
	diff, err = t.esHandler.TransformDiff(actual, expected)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.Empty(t.T(), diff)

	// When transform is not the same
	expected.Description = "plop"
	diff, err = t.esHandler.TransformDiff(actual, expected)
	if err != nil {
		t.Fail(err.Error())
	}
	assert.NotEmpty(t.T(), diff)

}
