package eshandler

import (
	"net/http"

	"github.com/pkg/errors"
)

// getRaw GETs through the underlying resty client; returns (nil, nil) on 404.
func (h *ElasticsearchHandlerImpl) getRaw(path string) ([]byte, error) {
	resp, err := h.client.RestyClient().R().Get(path)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() == http.StatusNotFound {
		return nil, nil
	}
	if resp.IsError() {
		// Truncate the error body to bound the error message size and limit
		// information disclosure from Elasticsearch error payloads (CWE-209).
		body := resp.Body()
		const maxErrorBody = 512
		if len(body) > maxErrorBody {
			body = body[:maxErrorBody]
		}
		return nil, errors.Errorf("Elasticsearch error %d: %s", resp.StatusCode(), string(body))
	}
	return resp.Body(), nil
}
