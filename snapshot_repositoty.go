package eshandler

import (
	"context"

	esapi "github.com/disaster37/elasticsearch/v9/api"
	"github.com/disaster37/generic-objectmatcher/patch"
)

// SnapshotRepositoryUpdate permit to create or update snapshot repository
func (h *ElasticsearchHandlerImpl) SnapshotRepositoryUpdate(name string, repository *esapi.SnapshotRepository) (err error) {
	_, err = h.client.Snapshot().CreateRepository(context.Background(), name, repository, nil)
	return err
}

// SnapshotRepositoryDelete permit to delete snapshot repository
func (h *ElasticsearchHandlerImpl) SnapshotRepositoryDelete(name string) (err error) {
	_, err = h.client.Snapshot().DeleteRepository(context.Background(), []string{name}, nil)
	return ignoreNotFound(err)
}

// SnapshotRepositoryGet permit to get snapshot repository
func (h *ElasticsearchHandlerImpl) SnapshotRepositoryGet(name string) (repository *esapi.SnapshotRepository, err error) {
	repos, err := h.client.Snapshot().GetRepository(context.Background(), []string{name}, nil)
	if err != nil {
		return nil, ignoreNotFound(err)
	}
	return repos[name], nil
}

// SnapshotRepositoryDiff permit to check if 2 repositories are the same
func (h *ElasticsearchHandlerImpl) SnapshotRepositoryDiff(actualObject, expectedObject, originalObject *esapi.SnapshotRepository) (patchResult *patch.PatchResult, err error) {
	return computeDiff(actualObject, expectedObject, originalObject)
}
