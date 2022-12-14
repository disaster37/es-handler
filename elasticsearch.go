package eshandler

import (
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
	ILMDiff(actual, expected *olivere.XPackIlmGetLifecycleResponse) (diff string, err error)

	// SLM scope
	SLMUpdate(name string, policy *SnapshotLifecyclePolicySpec) (err error)
	SLMDelete(name string) (err error)
	SLMGet(name string) (policy *SnapshotLifecyclePolicySpec, err error)
	SLMDiff(actual, expected *SnapshotLifecyclePolicySpec) (diff string, err error)

	// Snapshot repository scope
	SnapshotRepositoryUpdate(name string, repository *olivere.SnapshotRepositoryMetaData) (err error)
	SnapshotRepositoryDelete(name string) (err error)
	SnapshotRepositoryGet(name string) (repository *olivere.SnapshotRepositoryMetaData, err error)
	SnapshotRepositoryDiff(actual, expected *olivere.SnapshotRepositoryMetaData) (diff string, err error)

	// Role scope
	RoleUpdate(name string, role *XPackSecurityRole) (err error)
	RoleDelete(name string) (err error)
	RoleGet(name string) (role *XPackSecurityRole, err error)
	RoleDiff(actual, expected *XPackSecurityRole) (diff string, err error)

	// Role mapping scope
	RoleMappingUpdate(name string, roleMapping *olivere.XPackSecurityRoleMapping) (err error)
	RoleMappingDelete(name string) (err error)
	RoleMappingGet(name string) (roleMapping *olivere.XPackSecurityRoleMapping, err error)
	RoleMappingDiff(actual, expected *olivere.XPackSecurityRoleMapping) (diff string, err error)

	// User scope
	UserCreate(name string, user *olivere.XPackSecurityPutUserRequest) (err error)
	UserUpdate(name string, user *olivere.XPackSecurityPutUserRequest, isProtected ...bool) (err error)
	UserDelete(name string) (err error)
	UserGet(name string) (user *olivere.XPackSecurityUser, err error)
	UserDiff(actual, expected *olivere.XPackSecurityPutUserRequest) (diff string, err error)

	// Component template scope
	ComponentTemplateUpdate(name string, component *olivere.IndicesGetComponentTemplate) (err error)
	ComponentTemplateDelete(name string) (err error)
	ComponentTemplateGet(name string) (component *olivere.IndicesGetComponentTemplate, err error)
	ComponentTemplateDiff(actual, expected *olivere.IndicesGetComponentTemplate) (diff string, err error)

	// Index template scope
	IndexTemplateUpdate(name string, template *olivere.IndicesGetIndexTemplate) (err error)
	IndexTemplateDelete(name string) (err error)
	IndexTemplateGet(name string) (template *olivere.IndicesGetIndexTemplate, err error)
	IndexTemplateDiff(actual, expected *olivere.IndicesGetIndexTemplate) (diff string, err error)

	// ILM scope
	WatchUpdate(name string, watch *olivere.XPackWatch) (err error)
	WatchDelete(name string) (err error)
	WatchGet(name string) (watch *olivere.XPackWatch, err error)
	WatchDiff(actual, expected *olivere.XPackWatch) (diff string, err error)

	// Ingest pipline scope
	IngestPipelineUpdate(name string, pipeline *olivere.IngestGetPipeline) (err error)
	IngestPipelineDelete(name string) (err error)
	IngestPipelineGet(name string) (pipeline *olivere.IngestGetPipeline, err error)
	IngestPipelineDiff(actual, expected *olivere.IngestGetPipeline) (diff string, err error)

	// Transform scope
	TransformUpdate(name string, transform *Transform) (err error)
	TransformDelete(name string) (err error)
	TransformGet(name string) (transform *Transform, err error)
	TransformDiff(actual, expected *Transform) (diff string, err error)

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