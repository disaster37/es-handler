package eshandler

import (
	"context"

	elasticsearch "github.com/disaster37/elasticsearch/v9"
	esapi "github.com/disaster37/elasticsearch/v9/api"
	"github.com/disaster37/generic-objectmatcher/patch"
	"github.com/pkg/errors"
)

// SecurityPutUserRequest is the user create/update request body.
type SecurityPutUserRequest struct {
	Enabled      bool           `json:"enabled,omitempty"`
	Email        string         `json:"email,omitempty"`
	FullName     string         `json:"full_name,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	Password     string         `json:"password,omitempty"`
	PasswordHash string         `json:"password_hash,omitempty"`
	Roles        []string       `json:"roles,omitempty"`
}

// UserCreate permit to create new user
func (h *ElasticsearchHandlerImpl) UserCreate(name string, user *SecurityPutUserRequest) (err error) {
	_, err = h.client.Security().PutUser(context.Background(), name, user)
	return err
}

// UserUpdate permit to update the user
func (h *ElasticsearchHandlerImpl) UserUpdate(name string, user *SecurityPutUserRequest, isProtected ...bool) (err error) {
	isP := len(isProtected) > 0 && isProtected[0]

	//check if need to update password
	if user.Password != "" || user.PasswordHash != "" {
		payload := make(map[string]string)
		if user.Password != "" {
			payload["password"] = user.Password
		} else {
			payload["password_hash"] = user.PasswordHash
		}

		if _, err := h.client.Security().ChangePassword(context.Background(), name, payload); err != nil {
			return errors.Wrapf(err, "Error when change password for user %s", name)
		}

		h.log.Infof("Updated user password %s successfully", name)
	}

	// Not update use if is protected
	if isP {
		return nil
	}

	user.Password = ""
	user.PasswordHash = ""
	return h.UserCreate(name, user)
}

// UserDelete permit to delete the user
func (h *ElasticsearchHandlerImpl) UserDelete(name string) (err error) {
	_, err = h.client.Security().DeleteUser(context.Background(), name)
	if err != nil {
		if elasticsearch.IsNotFound(err) {
			return nil
		}
		return err
	}

	h.log.Infof("Deleted user %s successfully", name)

	return nil
}

// UserGet permot to get the user
func (h *ElasticsearchHandlerImpl) UserGet(name string) (user *esapi.SecurityUser, err error) {
	users, err := h.client.Security().GetUser(context.Background(), []string{name})
	if err != nil {
		return nil, ignoreNotFound(err)
	}

	h.log.Infof("Read user %s successfully", name)

	return users[name], nil
}

// UserDiff permit to check if 2 users are the same
func (h *ElasticsearchHandlerImpl) UserDiff(actualObject, expectedObject, originalObject *SecurityPutUserRequest) (patchResult *patch.PatchResult, err error) {
	return computeDiff(actualObject, expectedObject, originalObject)
}
