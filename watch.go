package eshandler

import (
	"context"

	"github.com/disaster37/generic-objectmatcher/patch"
)

// XPackWatch is a watch object.
type XPackWatch = map[string]any

// WatchUpdate permit to update or create watch
func (h *ElasticsearchHandlerImpl) WatchUpdate(name string, watch *XPackWatch) (err error) {
	_, err = h.client.Watcher().PutWatch(context.Background(), name, watch, nil)
	return err
}

// WatchDelete permit to delete watch
func (h *ElasticsearchHandlerImpl) WatchDelete(name string) (err error) {
	h.log.Debugf("Name: %s", name)
	_, err = h.client.Watcher().DeleteWatch(context.Background(), name)
	return ignoreNotFound(err)
}

// WatchGet permit to get watch
func (h *ElasticsearchHandlerImpl) WatchGet(name string) (watch *XPackWatch, err error) {
	h.log.Debugf("Name: %s", name)

	resp, err := h.client.Watcher().GetWatch(context.Background(), name)
	if err != nil {
		return nil, ignoreNotFound(err)
	}

	if resp == nil || !resp.Found {
		return nil, nil
	}

	return &resp.Watch, nil
}

// WatchDiff permit to check if 2 watch are the same
func (h *ElasticsearchHandlerImpl) WatchDiff(actualObject, expectedObject, originalObject *XPackWatch) (patchResult *patch.PatchResult, err error) {
	return computeDiff(actualObject, expectedObject, originalObject)
}
