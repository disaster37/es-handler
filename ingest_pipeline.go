package eshandler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/disaster37/generic-objectmatcher/patch"
	jsonIterator "github.com/json-iterator/go"
	olivere "github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
)

// IngestPipelineUpdate permit to create or update ingest pipeline
func (h *ElasticsearchHandlerImpl) IngestPipelineUpdate(name string, pipeline *olivere.IngestGetPipeline) (err error) {

	data, err := json.Marshal(pipeline)
	if err != nil {
		return err
	}

	res, err := h.client.API.Ingest.PutPipeline(
		name,
		bytes.NewReader(data),
		h.client.API.Ingest.PutPipeline.WithContext(context.Background()),
		h.client.API.Ingest.PutPipeline.WithPretty(),
	)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		return errors.Errorf("Error when add ingest pipeline %s: %s", name, res.String())
	}

	return nil

}

// IngestPipelineDelete permit to delete ingest pipeline
func (h *ElasticsearchHandlerImpl) IngestPipelineDelete(name string) (err error) {

	res, err := h.client.API.Ingest.DeletePipeline(
		name,
		h.client.API.Ingest.DeletePipeline.WithContext(context.Background()),
		h.client.API.Ingest.DeletePipeline.WithPretty(),
	)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return nil
		}
		return errors.Errorf("Error when delete ingest pipeline %s: %s", name, res.String())

	}

	return nil
}

// IngestPipelineGet permit to get ingest pipeline
func (h *ElasticsearchHandlerImpl) IngestPipelineGet(name string) (pipeline *olivere.IngestGetPipeline, err error) {

	res, err := h.client.API.Ingest.GetPipeline(
		h.client.API.Ingest.GetPipeline.WithPipelineID(name),
		h.client.API.Ingest.GetPipeline.WithContext(context.Background()),
		h.client.API.Ingest.GetPipeline.WithPretty(),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.IsError() {
		if res.StatusCode == 404 {
			return nil, nil
		}
		return nil, errors.Errorf("Error when get ingest pipeline %s: %s", name, res.String())

	}
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	pipelineResp := olivere.IngestGetPipelineResponse{}
	if err := json.Unmarshal(b, &pipelineResp); err != nil {
		return nil, err
	}

	return pipelineResp[name], nil
}

// IngestPipelineDiff permit to check if 2 ingest pipeline are the same
func (h *ElasticsearchHandlerImpl) IngestPipelineDiff(actualObject, expectedObject, originalObject *olivere.IngestGetPipeline) (patchResult *patch.PatchResult, err error) {
	// If not yet exist
	if actualObject == nil {
		expected, err := jsonIterator.ConfigCompatibleWithStandardLibrary.Marshal(expectedObject)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to convert expected object to byte sequence")
		}

		return &patch.PatchResult{
			Patch:    expected,
			Current:  expected,
			Modified: expected,
			Original: nil,
			Patched:  expectedObject,
		}, nil
	}

	return patch.DefaultPatchMaker.Calculate(actualObject, expectedObject, originalObject)
}
