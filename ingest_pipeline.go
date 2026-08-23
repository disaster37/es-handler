package eshandler

import (
	"context"

	esapi "github.com/disaster37/elasticsearch/v9/api"
	"github.com/disaster37/generic-objectmatcher/patch"
)

// IngestPipelineUpdate permit to create or update ingest pipeline
func (h *ElasticsearchHandlerImpl) IngestPipelineUpdate(name string, pipeline *esapi.IngestPipeline) (err error) {
	req := &esapi.IngestPutPipelineRequest{Id: name, Body: pipeline}
	_, err = h.client.Ingest().PutPipeline(context.Background(), req)
	return err
}

// IngestPipelineDelete permit to delete ingest pipeline
func (h *ElasticsearchHandlerImpl) IngestPipelineDelete(name string) (err error) {
	_, err = h.client.Ingest().DeletePipeline(context.Background(), name, nil)
	return ignoreNotFound(err)
}

// IngestPipelineGet permit to get ingest pipeline
func (h *ElasticsearchHandlerImpl) IngestPipelineGet(name string) (pipeline *esapi.IngestPipeline, err error) {
	pipelines, err := h.client.Ingest().GetPipeline(context.Background(), []string{name}, nil)
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	return pipelines[name], nil
}

// IngestPipelineDiff permit to check if 2 ingest pipeline are the same
func (h *ElasticsearchHandlerImpl) IngestPipelineDiff(actualObject, expectedObject, originalObject *esapi.IngestPipeline) (patchResult *patch.PatchResult, err error) {
	return computeDiff(actualObject, expectedObject, originalObject)
}
