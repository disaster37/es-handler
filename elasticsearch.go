package eshandler

import (
	"github.com/disaster37/es-handler/v8/patch"
	elastic "github.com/elastic/go-elasticsearch/v8"
	olivere "github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
)

type ElasticsearchHandler interface {
	Client() (client *elastic.Client)

	// License scope
	LicenseUpdate(license string) (err error)
	LicenseDelete() (err error)
	LicenseGet() (license *olivere.XPackInfoLicense, err error)
	LicenseDiff(actual, expected *olivere.XPackInfoLicense) (diff bool)
	LicenseEnableBasic() (err error)

	// ILM scope
	ILMUpdate(name string, policy *olivere.XPackIlmGetLifecycleResponse) (err error)
	ILMDelete(name string) (err error)
	ILMGet(name string) (policy *olivere.XPackIlmGetLifecycleResponse, err error)
	ILMDiff(actualObject, expectedObject, originalObject *olivere.XPackIlmGetLifecycleResponse) (patchResult *patch.PatchResult, err error)

	// SLM scope
	SLMUpdate(name string, policy *SnapshotLifecyclePolicySpec) (err error)
	SLMDelete(name string) (err error)
	SLMGet(name string) (policy *SnapshotLifecyclePolicySpec, err error)
	SLMDiff(actualObject, expectedObject, originalObject *SnapshotLifecyclePolicySpec) (patchResult *patch.PatchResult, err error)

	// Snapshot repository scope
	SnapshotRepositoryUpdate(name string, repository *olivere.SnapshotRepositoryMetaData) (err error)
	SnapshotRepositoryDelete(name string) (err error)
	SnapshotRepositoryGet(name string) (repository *olivere.SnapshotRepositoryMetaData, err error)
	SnapshotRepositoryDiff(actualObject, expectedObject, originalObject *olivere.SnapshotRepositoryMetaData) (patchResult *patch.PatchResult, err error)

	// Role scope
	RoleUpdate(name string, role *XPackSecurityRole) (err error)
	RoleDelete(name string) (err error)
	RoleGet(name string) (role *XPackSecurityRole, err error)
	RoleDiff(actualObject, expectedObject, originalObject *XPackSecurityRole) (patchResult *patch.PatchResult, err error)

	// Role mapping scope
	RoleMappingUpdate(name string, roleMapping *olivere.XPackSecurityRoleMapping) (err error)
	RoleMappingDelete(name string) (err error)
	RoleMappingGet(name string) (roleMapping *olivere.XPackSecurityRoleMapping, err error)
	RoleMappingDiff(actualObject, expectedObject, originalObject *olivere.XPackSecurityRoleMapping) (patchResult *patch.PatchResult, err error)

	// User scope
	UserCreate(name string, user *olivere.XPackSecurityPutUserRequest) (err error)
	UserUpdate(name string, user *olivere.XPackSecurityPutUserRequest, isProtected ...bool) (err error)
	UserDelete(name string) (err error)
	UserGet(name string) (user *olivere.XPackSecurityUser, err error)
	UserDiff(actualObject, expectedObject, originalObject *olivere.XPackSecurityPutUserRequest) (patchResult *patch.PatchResult, err error)

	// Component template scope
	ComponentTemplateUpdate(name string, component *olivere.IndicesGetComponentTemplate) (err error)
	ComponentTemplateDelete(name string) (err error)
	ComponentTemplateGet(name string) (component *olivere.IndicesGetComponentTemplate, err error)
	ComponentTemplateDiff(actualObject, expectedObject, originalObject *olivere.IndicesGetComponentTemplate) (patchResult *patch.PatchResult, err error)

	// Index template scope
	IndexTemplateUpdate(name string, template *olivere.IndicesGetIndexTemplate) (err error)
	IndexTemplateDelete(name string) (err error)
	IndexTemplateGet(name string) (template *olivere.IndicesGetIndexTemplate, err error)
	IndexTemplateDiff(actualObject, expectedObject, originalObject *olivere.IndicesGetIndexTemplate) (patchResult *patch.PatchResult, err error)

	// ILM scope
	WatchUpdate(name string, watch *olivere.XPackWatch) (err error)
	WatchDelete(name string) (err error)
	WatchGet(name string) (watch *olivere.XPackWatch, err error)
	WatchDiff(actualObject, expectedObject, originalObject *olivere.XPackWatch) (patchResult *patch.PatchResult, err error)

	// Ingest pipline scope
	IngestPipelineUpdate(name string, pipeline *olivere.IngestGetPipeline) (err error)
	IngestPipelineDelete(name string) (err error)
	IngestPipelineGet(name string) (pipeline *olivere.IngestGetPipeline, err error)
	IngestPipelineDiff(actualObject, expectedObject, originalObject *olivere.IngestGetPipeline) (patchResult *patch.PatchResult, err error)

	// Transform scope
	TransformUpdate(name string, transform *Transform) (err error)
	TransformDelete(name string) (err error)
	TransformGet(name string) (transform *Transform, err error)
	TransformDiff(actualObject, expectedObject, originalObject *Transform) (patchResult *patch.PatchResult, err error)

	// Cluster scope
	ClusterHealth() (health *olivere.ClusterHealthResponse, err error)

	SetLogger(log *logrus.Entry)
}

type ElasticsearchHandlerImpl struct {
	client *elastic.Client
	log    *logrus.Entry
}

func NewElasticsearchHandler(cfg elastic.Config, log *logrus.Entry) (ElasticsearchHandler, error) {

	client, err := elastic.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return &ElasticsearchHandlerImpl{
		client: client,
		log:    log,
	}, nil
}

func (h *ElasticsearchHandlerImpl) SetLogger(log *logrus.Entry) {
	h.log = log
}

func (h *ElasticsearchHandlerImpl) Client() *elastic.Client {
	return h.client
}
