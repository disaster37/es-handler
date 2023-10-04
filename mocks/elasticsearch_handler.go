// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/disaster37/es-handler/v8 (interfaces: ElasticsearchHandler)
//
// Generated by this command:
//
//	mockgen --build_flags=--mod=mod -destination=mocks/elasticsearch_handler.go -package=mocks github.com/disaster37/es-handler/v8 ElasticsearchHandler
//
// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	eshandler "github.com/disaster37/es-handler/v8"
	patch "github.com/disaster37/generic-objectmatcher/patch"
	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	elastic "github.com/olivere/elastic/v7"
	logrus "github.com/sirupsen/logrus"
	gomock "go.uber.org/mock/gomock"
)

// MockElasticsearchHandler is a mock of ElasticsearchHandler interface.
type MockElasticsearchHandler struct {
	ctrl     *gomock.Controller
	recorder *MockElasticsearchHandlerMockRecorder
}

// MockElasticsearchHandlerMockRecorder is the mock recorder for MockElasticsearchHandler.
type MockElasticsearchHandlerMockRecorder struct {
	mock *MockElasticsearchHandler
}

// NewMockElasticsearchHandler creates a new mock instance.
func NewMockElasticsearchHandler(ctrl *gomock.Controller) *MockElasticsearchHandler {
	mock := &MockElasticsearchHandler{ctrl: ctrl}
	mock.recorder = &MockElasticsearchHandlerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockElasticsearchHandler) EXPECT() *MockElasticsearchHandlerMockRecorder {
	return m.recorder
}

// Client mocks base method.
func (m *MockElasticsearchHandler) Client() *elasticsearch.Client {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Client")
	ret0, _ := ret[0].(*elasticsearch.Client)
	return ret0
}

// Client indicates an expected call of Client.
func (mr *MockElasticsearchHandlerMockRecorder) Client() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Client", reflect.TypeOf((*MockElasticsearchHandler)(nil).Client))
}

// ClusterHealth mocks base method.
func (m *MockElasticsearchHandler) ClusterHealth() (*elastic.ClusterHealthResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ClusterHealth")
	ret0, _ := ret[0].(*elastic.ClusterHealthResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ClusterHealth indicates an expected call of ClusterHealth.
func (mr *MockElasticsearchHandlerMockRecorder) ClusterHealth() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ClusterHealth", reflect.TypeOf((*MockElasticsearchHandler)(nil).ClusterHealth))
}

// ComponentTemplateDelete mocks base method.
func (m *MockElasticsearchHandler) ComponentTemplateDelete(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ComponentTemplateDelete", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// ComponentTemplateDelete indicates an expected call of ComponentTemplateDelete.
func (mr *MockElasticsearchHandlerMockRecorder) ComponentTemplateDelete(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ComponentTemplateDelete", reflect.TypeOf((*MockElasticsearchHandler)(nil).ComponentTemplateDelete), arg0)
}

// ComponentTemplateDiff mocks base method.
func (m *MockElasticsearchHandler) ComponentTemplateDiff(arg0, arg1, arg2 *elastic.IndicesGetComponentTemplate) (*patch.PatchResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ComponentTemplateDiff", arg0, arg1, arg2)
	ret0, _ := ret[0].(*patch.PatchResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ComponentTemplateDiff indicates an expected call of ComponentTemplateDiff.
func (mr *MockElasticsearchHandlerMockRecorder) ComponentTemplateDiff(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ComponentTemplateDiff", reflect.TypeOf((*MockElasticsearchHandler)(nil).ComponentTemplateDiff), arg0, arg1, arg2)
}

// ComponentTemplateGet mocks base method.
func (m *MockElasticsearchHandler) ComponentTemplateGet(arg0 string) (*elastic.IndicesGetComponentTemplate, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ComponentTemplateGet", arg0)
	ret0, _ := ret[0].(*elastic.IndicesGetComponentTemplate)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ComponentTemplateGet indicates an expected call of ComponentTemplateGet.
func (mr *MockElasticsearchHandlerMockRecorder) ComponentTemplateGet(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ComponentTemplateGet", reflect.TypeOf((*MockElasticsearchHandler)(nil).ComponentTemplateGet), arg0)
}

// ComponentTemplateUpdate mocks base method.
func (m *MockElasticsearchHandler) ComponentTemplateUpdate(arg0 string, arg1 *elastic.IndicesGetComponentTemplate) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ComponentTemplateUpdate", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// ComponentTemplateUpdate indicates an expected call of ComponentTemplateUpdate.
func (mr *MockElasticsearchHandlerMockRecorder) ComponentTemplateUpdate(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ComponentTemplateUpdate", reflect.TypeOf((*MockElasticsearchHandler)(nil).ComponentTemplateUpdate), arg0, arg1)
}

// ILMDelete mocks base method.
func (m *MockElasticsearchHandler) ILMDelete(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ILMDelete", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// ILMDelete indicates an expected call of ILMDelete.
func (mr *MockElasticsearchHandlerMockRecorder) ILMDelete(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ILMDelete", reflect.TypeOf((*MockElasticsearchHandler)(nil).ILMDelete), arg0)
}

// ILMDiff mocks base method.
func (m *MockElasticsearchHandler) ILMDiff(arg0, arg1, arg2 *elastic.XPackIlmGetLifecycleResponse) (*patch.PatchResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ILMDiff", arg0, arg1, arg2)
	ret0, _ := ret[0].(*patch.PatchResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ILMDiff indicates an expected call of ILMDiff.
func (mr *MockElasticsearchHandlerMockRecorder) ILMDiff(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ILMDiff", reflect.TypeOf((*MockElasticsearchHandler)(nil).ILMDiff), arg0, arg1, arg2)
}

// ILMGet mocks base method.
func (m *MockElasticsearchHandler) ILMGet(arg0 string) (*elastic.XPackIlmGetLifecycleResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ILMGet", arg0)
	ret0, _ := ret[0].(*elastic.XPackIlmGetLifecycleResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ILMGet indicates an expected call of ILMGet.
func (mr *MockElasticsearchHandlerMockRecorder) ILMGet(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ILMGet", reflect.TypeOf((*MockElasticsearchHandler)(nil).ILMGet), arg0)
}

// ILMUpdate mocks base method.
func (m *MockElasticsearchHandler) ILMUpdate(arg0 string, arg1 *elastic.XPackIlmGetLifecycleResponse) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ILMUpdate", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// ILMUpdate indicates an expected call of ILMUpdate.
func (mr *MockElasticsearchHandlerMockRecorder) ILMUpdate(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ILMUpdate", reflect.TypeOf((*MockElasticsearchHandler)(nil).ILMUpdate), arg0, arg1)
}

// IndexTemplateDelete mocks base method.
func (m *MockElasticsearchHandler) IndexTemplateDelete(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IndexTemplateDelete", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// IndexTemplateDelete indicates an expected call of IndexTemplateDelete.
func (mr *MockElasticsearchHandlerMockRecorder) IndexTemplateDelete(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IndexTemplateDelete", reflect.TypeOf((*MockElasticsearchHandler)(nil).IndexTemplateDelete), arg0)
}

// IndexTemplateDiff mocks base method.
func (m *MockElasticsearchHandler) IndexTemplateDiff(arg0, arg1, arg2 *elastic.IndicesGetIndexTemplate) (*patch.PatchResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IndexTemplateDiff", arg0, arg1, arg2)
	ret0, _ := ret[0].(*patch.PatchResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IndexTemplateDiff indicates an expected call of IndexTemplateDiff.
func (mr *MockElasticsearchHandlerMockRecorder) IndexTemplateDiff(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IndexTemplateDiff", reflect.TypeOf((*MockElasticsearchHandler)(nil).IndexTemplateDiff), arg0, arg1, arg2)
}

// IndexTemplateGet mocks base method.
func (m *MockElasticsearchHandler) IndexTemplateGet(arg0 string) (*elastic.IndicesGetIndexTemplate, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IndexTemplateGet", arg0)
	ret0, _ := ret[0].(*elastic.IndicesGetIndexTemplate)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IndexTemplateGet indicates an expected call of IndexTemplateGet.
func (mr *MockElasticsearchHandlerMockRecorder) IndexTemplateGet(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IndexTemplateGet", reflect.TypeOf((*MockElasticsearchHandler)(nil).IndexTemplateGet), arg0)
}

// IndexTemplateUpdate mocks base method.
func (m *MockElasticsearchHandler) IndexTemplateUpdate(arg0 string, arg1 *elastic.IndicesGetIndexTemplate) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IndexTemplateUpdate", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// IndexTemplateUpdate indicates an expected call of IndexTemplateUpdate.
func (mr *MockElasticsearchHandlerMockRecorder) IndexTemplateUpdate(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IndexTemplateUpdate", reflect.TypeOf((*MockElasticsearchHandler)(nil).IndexTemplateUpdate), arg0, arg1)
}

// IngestPipelineDelete mocks base method.
func (m *MockElasticsearchHandler) IngestPipelineDelete(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IngestPipelineDelete", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// IngestPipelineDelete indicates an expected call of IngestPipelineDelete.
func (mr *MockElasticsearchHandlerMockRecorder) IngestPipelineDelete(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IngestPipelineDelete", reflect.TypeOf((*MockElasticsearchHandler)(nil).IngestPipelineDelete), arg0)
}

// IngestPipelineDiff mocks base method.
func (m *MockElasticsearchHandler) IngestPipelineDiff(arg0, arg1, arg2 *elastic.IngestGetPipeline) (*patch.PatchResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IngestPipelineDiff", arg0, arg1, arg2)
	ret0, _ := ret[0].(*patch.PatchResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IngestPipelineDiff indicates an expected call of IngestPipelineDiff.
func (mr *MockElasticsearchHandlerMockRecorder) IngestPipelineDiff(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IngestPipelineDiff", reflect.TypeOf((*MockElasticsearchHandler)(nil).IngestPipelineDiff), arg0, arg1, arg2)
}

// IngestPipelineGet mocks base method.
func (m *MockElasticsearchHandler) IngestPipelineGet(arg0 string) (*elastic.IngestGetPipeline, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IngestPipelineGet", arg0)
	ret0, _ := ret[0].(*elastic.IngestGetPipeline)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IngestPipelineGet indicates an expected call of IngestPipelineGet.
func (mr *MockElasticsearchHandlerMockRecorder) IngestPipelineGet(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IngestPipelineGet", reflect.TypeOf((*MockElasticsearchHandler)(nil).IngestPipelineGet), arg0)
}

// IngestPipelineUpdate mocks base method.
func (m *MockElasticsearchHandler) IngestPipelineUpdate(arg0 string, arg1 *elastic.IngestGetPipeline) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IngestPipelineUpdate", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// IngestPipelineUpdate indicates an expected call of IngestPipelineUpdate.
func (mr *MockElasticsearchHandlerMockRecorder) IngestPipelineUpdate(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IngestPipelineUpdate", reflect.TypeOf((*MockElasticsearchHandler)(nil).IngestPipelineUpdate), arg0, arg1)
}

// LicenseDelete mocks base method.
func (m *MockElasticsearchHandler) LicenseDelete() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LicenseDelete")
	ret0, _ := ret[0].(error)
	return ret0
}

// LicenseDelete indicates an expected call of LicenseDelete.
func (mr *MockElasticsearchHandlerMockRecorder) LicenseDelete() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LicenseDelete", reflect.TypeOf((*MockElasticsearchHandler)(nil).LicenseDelete))
}

// LicenseDiff mocks base method.
func (m *MockElasticsearchHandler) LicenseDiff(arg0, arg1 *elastic.XPackInfoLicense) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LicenseDiff", arg0, arg1)
	ret0, _ := ret[0].(bool)
	return ret0
}

// LicenseDiff indicates an expected call of LicenseDiff.
func (mr *MockElasticsearchHandlerMockRecorder) LicenseDiff(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LicenseDiff", reflect.TypeOf((*MockElasticsearchHandler)(nil).LicenseDiff), arg0, arg1)
}

// LicenseEnableBasic mocks base method.
func (m *MockElasticsearchHandler) LicenseEnableBasic() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LicenseEnableBasic")
	ret0, _ := ret[0].(error)
	return ret0
}

// LicenseEnableBasic indicates an expected call of LicenseEnableBasic.
func (mr *MockElasticsearchHandlerMockRecorder) LicenseEnableBasic() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LicenseEnableBasic", reflect.TypeOf((*MockElasticsearchHandler)(nil).LicenseEnableBasic))
}

// LicenseGet mocks base method.
func (m *MockElasticsearchHandler) LicenseGet() (*elastic.XPackInfoLicense, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LicenseGet")
	ret0, _ := ret[0].(*elastic.XPackInfoLicense)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// LicenseGet indicates an expected call of LicenseGet.
func (mr *MockElasticsearchHandlerMockRecorder) LicenseGet() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LicenseGet", reflect.TypeOf((*MockElasticsearchHandler)(nil).LicenseGet))
}

// LicenseUpdate mocks base method.
func (m *MockElasticsearchHandler) LicenseUpdate(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LicenseUpdate", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// LicenseUpdate indicates an expected call of LicenseUpdate.
func (mr *MockElasticsearchHandlerMockRecorder) LicenseUpdate(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LicenseUpdate", reflect.TypeOf((*MockElasticsearchHandler)(nil).LicenseUpdate), arg0)
}

// RoleDelete mocks base method.
func (m *MockElasticsearchHandler) RoleDelete(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RoleDelete", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// RoleDelete indicates an expected call of RoleDelete.
func (mr *MockElasticsearchHandlerMockRecorder) RoleDelete(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RoleDelete", reflect.TypeOf((*MockElasticsearchHandler)(nil).RoleDelete), arg0)
}

// RoleDiff mocks base method.
func (m *MockElasticsearchHandler) RoleDiff(arg0, arg1, arg2 *eshandler.XPackSecurityRole) (*patch.PatchResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RoleDiff", arg0, arg1, arg2)
	ret0, _ := ret[0].(*patch.PatchResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RoleDiff indicates an expected call of RoleDiff.
func (mr *MockElasticsearchHandlerMockRecorder) RoleDiff(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RoleDiff", reflect.TypeOf((*MockElasticsearchHandler)(nil).RoleDiff), arg0, arg1, arg2)
}

// RoleGet mocks base method.
func (m *MockElasticsearchHandler) RoleGet(arg0 string) (*eshandler.XPackSecurityRole, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RoleGet", arg0)
	ret0, _ := ret[0].(*eshandler.XPackSecurityRole)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RoleGet indicates an expected call of RoleGet.
func (mr *MockElasticsearchHandlerMockRecorder) RoleGet(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RoleGet", reflect.TypeOf((*MockElasticsearchHandler)(nil).RoleGet), arg0)
}

// RoleMappingDelete mocks base method.
func (m *MockElasticsearchHandler) RoleMappingDelete(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RoleMappingDelete", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// RoleMappingDelete indicates an expected call of RoleMappingDelete.
func (mr *MockElasticsearchHandlerMockRecorder) RoleMappingDelete(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RoleMappingDelete", reflect.TypeOf((*MockElasticsearchHandler)(nil).RoleMappingDelete), arg0)
}

// RoleMappingDiff mocks base method.
func (m *MockElasticsearchHandler) RoleMappingDiff(arg0, arg1, arg2 *elastic.XPackSecurityRoleMapping) (*patch.PatchResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RoleMappingDiff", arg0, arg1, arg2)
	ret0, _ := ret[0].(*patch.PatchResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RoleMappingDiff indicates an expected call of RoleMappingDiff.
func (mr *MockElasticsearchHandlerMockRecorder) RoleMappingDiff(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RoleMappingDiff", reflect.TypeOf((*MockElasticsearchHandler)(nil).RoleMappingDiff), arg0, arg1, arg2)
}

// RoleMappingGet mocks base method.
func (m *MockElasticsearchHandler) RoleMappingGet(arg0 string) (*elastic.XPackSecurityRoleMapping, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RoleMappingGet", arg0)
	ret0, _ := ret[0].(*elastic.XPackSecurityRoleMapping)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RoleMappingGet indicates an expected call of RoleMappingGet.
func (mr *MockElasticsearchHandlerMockRecorder) RoleMappingGet(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RoleMappingGet", reflect.TypeOf((*MockElasticsearchHandler)(nil).RoleMappingGet), arg0)
}

// RoleMappingUpdate mocks base method.
func (m *MockElasticsearchHandler) RoleMappingUpdate(arg0 string, arg1 *elastic.XPackSecurityRoleMapping) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RoleMappingUpdate", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// RoleMappingUpdate indicates an expected call of RoleMappingUpdate.
func (mr *MockElasticsearchHandlerMockRecorder) RoleMappingUpdate(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RoleMappingUpdate", reflect.TypeOf((*MockElasticsearchHandler)(nil).RoleMappingUpdate), arg0, arg1)
}

// RoleUpdate mocks base method.
func (m *MockElasticsearchHandler) RoleUpdate(arg0 string, arg1 *eshandler.XPackSecurityRole) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RoleUpdate", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// RoleUpdate indicates an expected call of RoleUpdate.
func (mr *MockElasticsearchHandlerMockRecorder) RoleUpdate(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RoleUpdate", reflect.TypeOf((*MockElasticsearchHandler)(nil).RoleUpdate), arg0, arg1)
}

// SLMDelete mocks base method.
func (m *MockElasticsearchHandler) SLMDelete(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SLMDelete", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SLMDelete indicates an expected call of SLMDelete.
func (mr *MockElasticsearchHandlerMockRecorder) SLMDelete(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SLMDelete", reflect.TypeOf((*MockElasticsearchHandler)(nil).SLMDelete), arg0)
}

// SLMDiff mocks base method.
func (m *MockElasticsearchHandler) SLMDiff(arg0, arg1, arg2 *eshandler.SnapshotLifecyclePolicySpec) (*patch.PatchResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SLMDiff", arg0, arg1, arg2)
	ret0, _ := ret[0].(*patch.PatchResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SLMDiff indicates an expected call of SLMDiff.
func (mr *MockElasticsearchHandlerMockRecorder) SLMDiff(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SLMDiff", reflect.TypeOf((*MockElasticsearchHandler)(nil).SLMDiff), arg0, arg1, arg2)
}

// SLMGet mocks base method.
func (m *MockElasticsearchHandler) SLMGet(arg0 string) (*eshandler.SnapshotLifecyclePolicySpec, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SLMGet", arg0)
	ret0, _ := ret[0].(*eshandler.SnapshotLifecyclePolicySpec)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SLMGet indicates an expected call of SLMGet.
func (mr *MockElasticsearchHandlerMockRecorder) SLMGet(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SLMGet", reflect.TypeOf((*MockElasticsearchHandler)(nil).SLMGet), arg0)
}

// SLMUpdate mocks base method.
func (m *MockElasticsearchHandler) SLMUpdate(arg0 string, arg1 *eshandler.SnapshotLifecyclePolicySpec) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SLMUpdate", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// SLMUpdate indicates an expected call of SLMUpdate.
func (mr *MockElasticsearchHandlerMockRecorder) SLMUpdate(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SLMUpdate", reflect.TypeOf((*MockElasticsearchHandler)(nil).SLMUpdate), arg0, arg1)
}

// SetLogger mocks base method.
func (m *MockElasticsearchHandler) SetLogger(arg0 *logrus.Entry) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetLogger", arg0)
}

// SetLogger indicates an expected call of SetLogger.
func (mr *MockElasticsearchHandlerMockRecorder) SetLogger(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetLogger", reflect.TypeOf((*MockElasticsearchHandler)(nil).SetLogger), arg0)
}

// SnapshotRepositoryDelete mocks base method.
func (m *MockElasticsearchHandler) SnapshotRepositoryDelete(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SnapshotRepositoryDelete", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SnapshotRepositoryDelete indicates an expected call of SnapshotRepositoryDelete.
func (mr *MockElasticsearchHandlerMockRecorder) SnapshotRepositoryDelete(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SnapshotRepositoryDelete", reflect.TypeOf((*MockElasticsearchHandler)(nil).SnapshotRepositoryDelete), arg0)
}

// SnapshotRepositoryDiff mocks base method.
func (m *MockElasticsearchHandler) SnapshotRepositoryDiff(arg0, arg1, arg2 *elastic.SnapshotRepositoryMetaData) (*patch.PatchResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SnapshotRepositoryDiff", arg0, arg1, arg2)
	ret0, _ := ret[0].(*patch.PatchResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SnapshotRepositoryDiff indicates an expected call of SnapshotRepositoryDiff.
func (mr *MockElasticsearchHandlerMockRecorder) SnapshotRepositoryDiff(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SnapshotRepositoryDiff", reflect.TypeOf((*MockElasticsearchHandler)(nil).SnapshotRepositoryDiff), arg0, arg1, arg2)
}

// SnapshotRepositoryGet mocks base method.
func (m *MockElasticsearchHandler) SnapshotRepositoryGet(arg0 string) (*elastic.SnapshotRepositoryMetaData, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SnapshotRepositoryGet", arg0)
	ret0, _ := ret[0].(*elastic.SnapshotRepositoryMetaData)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SnapshotRepositoryGet indicates an expected call of SnapshotRepositoryGet.
func (mr *MockElasticsearchHandlerMockRecorder) SnapshotRepositoryGet(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SnapshotRepositoryGet", reflect.TypeOf((*MockElasticsearchHandler)(nil).SnapshotRepositoryGet), arg0)
}

// SnapshotRepositoryUpdate mocks base method.
func (m *MockElasticsearchHandler) SnapshotRepositoryUpdate(arg0 string, arg1 *elastic.SnapshotRepositoryMetaData) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SnapshotRepositoryUpdate", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// SnapshotRepositoryUpdate indicates an expected call of SnapshotRepositoryUpdate.
func (mr *MockElasticsearchHandlerMockRecorder) SnapshotRepositoryUpdate(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SnapshotRepositoryUpdate", reflect.TypeOf((*MockElasticsearchHandler)(nil).SnapshotRepositoryUpdate), arg0, arg1)
}

// TransformDelete mocks base method.
func (m *MockElasticsearchHandler) TransformDelete(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TransformDelete", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// TransformDelete indicates an expected call of TransformDelete.
func (mr *MockElasticsearchHandlerMockRecorder) TransformDelete(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TransformDelete", reflect.TypeOf((*MockElasticsearchHandler)(nil).TransformDelete), arg0)
}

// TransformDiff mocks base method.
func (m *MockElasticsearchHandler) TransformDiff(arg0, arg1, arg2 *eshandler.Transform) (*patch.PatchResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TransformDiff", arg0, arg1, arg2)
	ret0, _ := ret[0].(*patch.PatchResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TransformDiff indicates an expected call of TransformDiff.
func (mr *MockElasticsearchHandlerMockRecorder) TransformDiff(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TransformDiff", reflect.TypeOf((*MockElasticsearchHandler)(nil).TransformDiff), arg0, arg1, arg2)
}

// TransformGet mocks base method.
func (m *MockElasticsearchHandler) TransformGet(arg0 string) (*eshandler.Transform, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TransformGet", arg0)
	ret0, _ := ret[0].(*eshandler.Transform)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TransformGet indicates an expected call of TransformGet.
func (mr *MockElasticsearchHandlerMockRecorder) TransformGet(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TransformGet", reflect.TypeOf((*MockElasticsearchHandler)(nil).TransformGet), arg0)
}

// TransformUpdate mocks base method.
func (m *MockElasticsearchHandler) TransformUpdate(arg0 string, arg1 *eshandler.Transform) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TransformUpdate", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// TransformUpdate indicates an expected call of TransformUpdate.
func (mr *MockElasticsearchHandlerMockRecorder) TransformUpdate(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TransformUpdate", reflect.TypeOf((*MockElasticsearchHandler)(nil).TransformUpdate), arg0, arg1)
}

// UserCreate mocks base method.
func (m *MockElasticsearchHandler) UserCreate(arg0 string, arg1 *elastic.XPackSecurityPutUserRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UserCreate", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UserCreate indicates an expected call of UserCreate.
func (mr *MockElasticsearchHandlerMockRecorder) UserCreate(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UserCreate", reflect.TypeOf((*MockElasticsearchHandler)(nil).UserCreate), arg0, arg1)
}

// UserDelete mocks base method.
func (m *MockElasticsearchHandler) UserDelete(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UserDelete", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// UserDelete indicates an expected call of UserDelete.
func (mr *MockElasticsearchHandlerMockRecorder) UserDelete(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UserDelete", reflect.TypeOf((*MockElasticsearchHandler)(nil).UserDelete), arg0)
}

// UserDiff mocks base method.
func (m *MockElasticsearchHandler) UserDiff(arg0, arg1, arg2 *elastic.XPackSecurityPutUserRequest) (*patch.PatchResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UserDiff", arg0, arg1, arg2)
	ret0, _ := ret[0].(*patch.PatchResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UserDiff indicates an expected call of UserDiff.
func (mr *MockElasticsearchHandlerMockRecorder) UserDiff(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UserDiff", reflect.TypeOf((*MockElasticsearchHandler)(nil).UserDiff), arg0, arg1, arg2)
}

// UserGet mocks base method.
func (m *MockElasticsearchHandler) UserGet(arg0 string) (*elastic.XPackSecurityUser, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UserGet", arg0)
	ret0, _ := ret[0].(*elastic.XPackSecurityUser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UserGet indicates an expected call of UserGet.
func (mr *MockElasticsearchHandlerMockRecorder) UserGet(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UserGet", reflect.TypeOf((*MockElasticsearchHandler)(nil).UserGet), arg0)
}

// UserUpdate mocks base method.
func (m *MockElasticsearchHandler) UserUpdate(arg0 string, arg1 *elastic.XPackSecurityPutUserRequest, arg2 ...bool) error {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UserUpdate", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// UserUpdate indicates an expected call of UserUpdate.
func (mr *MockElasticsearchHandlerMockRecorder) UserUpdate(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UserUpdate", reflect.TypeOf((*MockElasticsearchHandler)(nil).UserUpdate), varargs...)
}

// WatchDelete mocks base method.
func (m *MockElasticsearchHandler) WatchDelete(arg0 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WatchDelete", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// WatchDelete indicates an expected call of WatchDelete.
func (mr *MockElasticsearchHandlerMockRecorder) WatchDelete(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WatchDelete", reflect.TypeOf((*MockElasticsearchHandler)(nil).WatchDelete), arg0)
}

// WatchDiff mocks base method.
func (m *MockElasticsearchHandler) WatchDiff(arg0, arg1, arg2 *elastic.XPackWatch) (*patch.PatchResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WatchDiff", arg0, arg1, arg2)
	ret0, _ := ret[0].(*patch.PatchResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// WatchDiff indicates an expected call of WatchDiff.
func (mr *MockElasticsearchHandlerMockRecorder) WatchDiff(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WatchDiff", reflect.TypeOf((*MockElasticsearchHandler)(nil).WatchDiff), arg0, arg1, arg2)
}

// WatchGet mocks base method.
func (m *MockElasticsearchHandler) WatchGet(arg0 string) (*elastic.XPackWatch, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WatchGet", arg0)
	ret0, _ := ret[0].(*elastic.XPackWatch)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// WatchGet indicates an expected call of WatchGet.
func (mr *MockElasticsearchHandlerMockRecorder) WatchGet(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WatchGet", reflect.TypeOf((*MockElasticsearchHandler)(nil).WatchGet), arg0)
}

// WatchUpdate mocks base method.
func (m *MockElasticsearchHandler) WatchUpdate(arg0 string, arg1 *elastic.XPackWatch) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WatchUpdate", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// WatchUpdate indicates an expected call of WatchUpdate.
func (mr *MockElasticsearchHandlerMockRecorder) WatchUpdate(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WatchUpdate", reflect.TypeOf((*MockElasticsearchHandler)(nil).WatchUpdate), arg0, arg1)
}
