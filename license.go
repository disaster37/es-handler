package eshandler

import (
	"context"
	"encoding/json"

	elasticsearch "github.com/disaster37/elasticsearch/v9"
	esapi "github.com/disaster37/elasticsearch/v9/api"
)

// LicenseEnableBasic permit to enable basic license
func (h *ElasticsearchHandlerImpl) LicenseEnableBasic() (err error) {
	data, err := h.client.License().GetBasicStatus(context.Background())
	if err != nil {
		return err
	}

	h.log.Debugf("Result when get basic license status: %s", string(data))

	res := make(map[string]interface{})
	if err = json.Unmarshal(data, &res); err != nil {
		return err
	}

	if eligible, ok := res["eligible_to_start_basic"].(bool); ok && !eligible {
		h.log.Infof("Basic license is already enabled")
		return nil
	}

	ack := true
	_, err = h.client.License().PostStartBasic(context.Background(), &esapi.LicensePostParams{Acknowledge: &ack})
	return err
}

// LicenseUpdate permit to add or update new license
func (h *ElasticsearchHandlerImpl) LicenseUpdate(license string) (err error) {
	ack := true
	_, err = h.client.License().Post(context.Background(), license, &esapi.LicensePostParams{Acknowledge: &ack})
	return err
}

// LicenseDelete permit to delete the current license
func (h *ElasticsearchHandlerImpl) LicenseDelete() (err error) {
	_, err = h.client.License().Delete(context.Background())
	if err != nil {
		if elasticsearch.IsNotFound(err) {
			h.log.Warnf("License not found, skip it")
			return nil
		}
		return err
	}

	return nil
}

// LicenseGet permit to get the current license
func (h *ElasticsearchHandlerImpl) LicenseGet() (license *esapi.LicenseInfo, err error) {
	resp, err := h.client.License().Get(context.Background(), nil)
	if err != nil {
		if elasticsearch.IsNotFound(err) {
			h.log.Warnf("License not found")
			return nil, nil
		}
		return nil, err
	}

	return &resp.License, nil
}

// LicenseDiff permit to compare actual license with expected license.
// It only compare the UID if expected is not basic license
func (h *ElasticsearchHandlerImpl) LicenseDiff(actual, expected *esapi.LicenseInfo) (isDiff bool) {

	if actual == nil {
		return true
	}

	// Don't check UID is basic license
	if expected.Type == "basic" {
		return actual.Type != expected.Type
	}

	return actual.UID != expected.UID
}
