package eshandler

import (
	elasticsearch "github.com/disaster37/elasticsearch/v9"
	esapi "github.com/disaster37/elasticsearch/v9/api"
	localpatch "github.com/disaster37/es-handler/v9/patch"
	"github.com/disaster37/generic-objectmatcher/patch"
	jsonIterator "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type ElasticsearchHandler interface {
	Client() (client elasticsearch.Client)

	// License scope
	LicenseUpdate(license string) (err error)
	LicenseDelete() (err error)
	LicenseGet() (license *esapi.LicenseInfo, err error)
	LicenseDiff(actual, expected *esapi.LicenseInfo) (diff bool)
	LicenseEnableBasic() (err error)

	// ILM scope
	ILMUpdate(name string, policy *esapi.IlmPolicy) (err error)
	ILMDelete(name string) (err error)
	ILMGet(name string) (policy *esapi.IlmPolicy, err error)
	ILMDiff(actualObject, expectedObject, originalObject *esapi.IlmPolicy) (patchResult *patch.PatchResult, err error)

	// SLM scope
	SLMUpdate(name string, policy *SnapshotLifecyclePolicySpec) (err error)
	SLMDelete(name string) (err error)
	SLMGet(name string) (policy *SnapshotLifecyclePolicySpec, err error)
	SLMDiff(actualObject, expectedObject, originalObject *SnapshotLifecyclePolicySpec) (patchResult *patch.PatchResult, err error)

	// Snapshot repository scope
	SnapshotRepositoryUpdate(name string, repository *esapi.SnapshotRepository) (err error)
	SnapshotRepositoryDelete(name string) (err error)
	SnapshotRepositoryGet(name string) (repository *esapi.SnapshotRepository, err error)
	SnapshotRepositoryDiff(actualObject, expectedObject, originalObject *esapi.SnapshotRepository) (patchResult *patch.PatchResult, err error)

	// Role scope
	RoleUpdate(name string, role *XPackSecurityRole) (err error)
	RoleDelete(name string) (err error)
	RoleGet(name string) (role *XPackSecurityRole, err error)
	RoleDiff(actualObject, expectedObject, originalObject *XPackSecurityRole) (patchResult *patch.PatchResult, err error)

	// Role mapping scope
	RoleMappingUpdate(name string, roleMapping *esapi.SecurityRoleMapping) (err error)
	RoleMappingDelete(name string) (err error)
	RoleMappingGet(name string) (roleMapping *esapi.SecurityRoleMapping, err error)
	RoleMappingDiff(actualObject, expectedObject, originalObject *esapi.SecurityRoleMapping) (patchResult *patch.PatchResult, err error)

	// User scope
	UserCreate(name string, user *SecurityPutUserRequest) (err error)
	UserUpdate(name string, user *SecurityPutUserRequest, isProtected ...bool) (err error)
	UserDelete(name string) (err error)
	UserGet(name string) (user *esapi.SecurityUser, err error)
	UserDiff(actualObject, expectedObject, originalObject *SecurityPutUserRequest) (patchResult *patch.PatchResult, err error)

	// Component template scope
	ComponentTemplateUpdate(name string, component *localpatch.ComponentTemplate) (err error)
	ComponentTemplateDelete(name string) (err error)
	ComponentTemplateGet(name string) (component *localpatch.ComponentTemplate, err error)
	ComponentTemplateDiff(actualObject, expectedObject, originalObject *localpatch.ComponentTemplate) (patchResult *patch.PatchResult, err error)

	// Index template scope
	IndexTemplateUpdate(name string, template *localpatch.IndexTemplate) (err error)
	IndexTemplateDelete(name string) (err error)
	IndexTemplateGet(name string) (template *localpatch.IndexTemplate, err error)
	IndexTemplateDiff(actualObject, expectedObject, originalObject *localpatch.IndexTemplate) (patchResult *patch.PatchResult, err error)

	// Watch scope
	WatchUpdate(name string, watch *XPackWatch) (err error)
	WatchDelete(name string) (err error)
	WatchGet(name string) (watch *XPackWatch, err error)
	WatchDiff(actualObject, expectedObject, originalObject *XPackWatch) (patchResult *patch.PatchResult, err error)

	// Ingest pipline scope
	IngestPipelineUpdate(name string, pipeline *esapi.IngestPipeline) (err error)
	IngestPipelineDelete(name string) (err error)
	IngestPipelineGet(name string) (pipeline *esapi.IngestPipeline, err error)
	IngestPipelineDiff(actualObject, expectedObject, originalObject *esapi.IngestPipeline) (patchResult *patch.PatchResult, err error)

	// Transform scope
	TransformUpdate(name string, transform *Transform) (err error)
	TransformDelete(name string) (err error)
	TransformGet(name string) (transform *Transform, err error)
	TransformDiff(actualObject, expectedObject, originalObject *Transform) (patchResult *patch.PatchResult, err error)

	// Cluster scope
	ClusterHealth() (health *esapi.ClusterHealthResponse, err error)
	EnableRoutingRebalance() (err error)
	DisableRoutingRebalance() (err error)
	EnableRoutingAllocation() (err error)
	DisableRoutingAllocation() (err error)

	SetLogger(log *logrus.Entry)
}

type ElasticsearchHandlerImpl struct {
	client elasticsearch.Client
	log    *logrus.Entry
}

func NewElasticsearchHandler(cfg *elasticsearch.Config, log *logrus.Entry) (ElasticsearchHandler, error) {
	if log == nil {
		log = logrus.NewEntry(logrus.New())
	}
	client, err := elasticsearch.New(cfg, log)
	if err != nil {
		return nil, err
	}
	return &ElasticsearchHandlerImpl{client: client, log: log}, nil
}

func (h *ElasticsearchHandlerImpl) SetLogger(log *logrus.Entry) {
	h.log = log
}

func (h *ElasticsearchHandlerImpl) Client() elasticsearch.Client {
	return h.client
}

// ignoreNotFound returns nil when err is a 404 Not Found error, keeping Delete
// idempotent and letting Get report "no resource". Any other error (including a
// nil error) is returned unchanged.
func ignoreNotFound(err error) error {
	if elasticsearch.IsNotFound(err) {
		return nil
	}
	return err
}

// computeDiff computes the three-way patch between actual and expected objects.
// When the resource does not exist yet (actual == nil), it returns a patch that
// creates the expected object.
func computeDiff[T any](actual, expected, original *T, opts ...patch.CalculateOption) (patchResult *patch.PatchResult, err error) {
	if actual == nil {
		b, err := jsonIterator.ConfigCompatibleWithStandardLibrary.Marshal(expected)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to convert expected object to byte sequence")
		}
		return &patch.PatchResult{
			Patch:    b,
			Current:  b,
			Modified: b,
			Original: nil,
			Patched:  expected,
		}, nil
	}

	return patch.DefaultPatchMaker.Calculate(actual, expected, original, opts...)
}
