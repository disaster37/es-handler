package eshandler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/disaster37/es-handler/v8/patch"
	jsonIterator "github.com/json-iterator/go"
	olivere "github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
)

// SnapshotRepositoryUpdate permit to create or update snapshot repository
func (h *ElasticsearchHandlerImpl) SnapshotRepositoryUpdate(name string, repository *olivere.SnapshotRepositoryMetaData) (err error) {

	b, err := json.Marshal(repository)
	if err != nil {
		return err
	}

	res, err := h.client.API.Snapshot.CreateRepository(
		name,
		bytes.NewReader(b),
		h.client.API.Snapshot.CreateRepository.WithContext(context.Background()),
		h.client.API.Snapshot.CreateRepository.WithPretty(),
	)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		return errors.Errorf("Error when add snapshot repository %s: %s", name, res.String())
	}

	return nil
}

// SnapshotRepositoryDelete permit to delete snapshot repository
func (h *ElasticsearchHandlerImpl) SnapshotRepositoryDelete(name string) (err error) {

	res, err := h.client.API.Snapshot.DeleteRepository(
		[]string{name},
		h.client.API.Snapshot.DeleteRepository.WithContext(context.Background()),
		h.client.API.Snapshot.DeleteRepository.WithPretty(),
	)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return nil
		}
		return errors.Errorf("Error when delete snapshot repository %s: %s", name, res.String())

	}

	return nil
}

// SnapshotRepositoryGet permit to get snapshot repository
func (h *ElasticsearchHandlerImpl) SnapshotRepositoryGet(name string) (repository *olivere.SnapshotRepositoryMetaData, err error) {

	res, err := h.client.API.Snapshot.GetRepository(
		h.client.API.Snapshot.GetRepository.WithContext(context.Background()),
		h.client.API.Snapshot.GetRepository.WithPretty(),
		h.client.API.Snapshot.GetRepository.WithRepository(name),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.IsError() {
		if res.StatusCode == 404 {
			return nil, nil
		}
		return nil, errors.Errorf("Error when get snapshot repository %s: %s", name, res.String())

	}
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	h.log.Debugf("Get Snapshot repository successfully:\n%s", string(b))

	snapshotRepository := make(olivere.SnapshotGetRepositoryResponse)
	err = json.Unmarshal(b, &snapshotRepository)
	if err != nil {
		return nil, err
	}

	return snapshotRepository[name], nil

}

// SnapshotRepositoryDiff permit to check if 2 repositories are the same
func (h *ElasticsearchHandlerImpl) SnapshotRepositoryDiff(actualObject, expectedObject, originalObject *olivere.SnapshotRepositoryMetaData) (patchResult *patch.PatchResult, err error) {
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
