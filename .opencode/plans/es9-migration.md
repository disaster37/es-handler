# es-handler → Elasticsearch v9 Client Migration Plan

## 1. Goal & Scope

Replace BOTH client dependencies with a single library:

- `github.com/elastic/go-elasticsearch/v8` (aliased `elastic`, official client — transport/API calls)
- `github.com/olivere/elastic/v7` (aliased `olivere`, used ONLY for request/response TYPES)

…with `github.com/disaster37/elasticsearch/v9` (module path `github.com/disaster37/elasticsearch/v9`, root package `elasticsearch`).

Also:
- Bump module path `github.com/disaster37/es-handler/v8` → `github.com/disaster37/es-handler/v9`.
- Target Elasticsearch 9.x in acceptance tests (no in-repo ES pin exists — see §12).
- Enhance test coverage (not-found, auth-failure, conflict, body-serialization assertions).
- **Preserve all public behavior/APIs**: method names & semantics unchanged; only referenced types change (plus `Client()` return type).

> **Toolchain note (hard requirement):** the target library declares `go 1.26` in its `go.mod` (verified). The es-handler `go` directive MUST be bumped to ≥ `1.26`; Go refuses to build when a dependency requires a newer Go. Implementer needs a Go ≥ 1.26 toolchain. Do NOT attempt to stay on `go 1.20`.

## 2. Target library facts (verified @ branch `v9`)

Root package (`package elasticsearch`):

- `func New(cfg *Config, logger *logrus.Entry) (Client, error)`
  - `New` does `http.DefaultTransport.(*http.Transport).Clone()` — a hard assertion, so it **MUST NOT be called after `httpmock.Activate()`**.
  - `New` requires a **non-nil** `*logrus.Entry`.
- `type Client interface`: `RestyClient() *resty.Client`, `Info()`, `Document()`, `Search()`, `Indices() api.IndicesService`, `Cluster() api.ClusterService`, `Ingest() api.IngestService`, `Snapshot() api.SnapshotService`, `SLM() api.SlmService`, `ILM() api.IlmService`, `Transform() api.TransformService`, `Security() api.SecurityService`, `Watcher() api.WatcherService`, `License() api.LicenseService`, … (only these are used).
- `type Config struct { URL string; Username, Password, APIKey, BearerToken string; TLSSkipVerify bool; AllowInsecureHTTP bool; CACert []byte; Timeout, IdleConnTimeout time.Duration; DisableHTTP2 bool; RetryCount int; RetryWaitTime, RetryMaxWaitTime time.Duration; RetryConditions []resty.RetryConditionFunc }` — **no `Transport`, no `Addresses`** (single `URL`).
- Root error helpers: `IsNotFound(err) bool` (404), `IsConflict(err) bool` (409), `IsUnauthorized(err) bool` (401).

`api` package (`package api`): all services are interfaces; **every method takes `ctx context.Context` as FIRST arg**. Writes take `body any` (`string`/`[]byte`/`json.RawMessage` are sent verbatim; structs/maps are marshaled by resty). Non-2xx → returns `*types.ElasticsearchError`; network errors returned unchanged. No `res.IsError()`/`res.StatusCode`/`res.Body`.

`types` package: `ElasticsearchError{Status int; Details *ElasticsearchErrorDetails}`, `AcknowledgedResponse{Acknowledged bool; ShardsAcknowledged bool; Index string}`, `IsNotFound/IsConflict/IsUnauthorized`.

### Exact service signatures used

```go
// ILM (api.IlmService)
GetLifecycle(ctx, policies []string) (map[string]*IlmPolicy, error)
PutLifecycle(ctx, policy string, body any) (*types.AcknowledgedResponse, error)
DeleteLifecycle(ctx, policy string) (*types.AcknowledgedResponse, error)
// IlmPolicy { Version int; ModifiedDate string; ModifiedDateMillis int64; Policy map[string]any `json:"policy,omitempty"`; InUse bool }

// SLM (api.SlmService)
GetLifecycle(ctx, policyIds []string) (map[string]*SlmPolicy, error)
PutLifecycle(ctx, policyId string, body any) (*types.AcknowledgedResponse, error)
DeleteLifecycle(ctx, policyId string) (*types.AcknowledgedResponse, error)
// SlmPolicy { ...; Policy map[string]any `json:"policy,omitempty"`; ... }  // .Policy holds the raw spec map

// Snapshot (api.SnapshotService)
CreateRepository(ctx, name string, body any, params *SnapshotCreateRepositoryParams) (*types.AcknowledgedResponse, error)
GetRepository(ctx, names []string, params *SnapshotGetRepositoryParams) (map[string]*SnapshotRepository, error)
DeleteRepository(ctx, names []string, params *SnapshotDeleteRepositoryParams) (*types.AcknowledgedResponse, error)
// SnapshotRepository { Type string `json:"type"`; Settings map[string]string `json:"settings,omitempty"`; UUID string }

// Security (api.SecurityService)
GetUser(ctx, usernames []string) (map[string]*SecurityUser, error)
PutUser(ctx, username string, body any) (*SecurityCreateResponse, error)
DeleteUser(ctx, username string) (*types.AcknowledgedResponse, error)
ChangePassword(ctx, username string, body any) (*types.AcknowledgedResponse, error) // POST /_security/user/{u}/_password
GetRole(ctx, names []string) (map[string]*SecurityRole, error)
PutRole(ctx, name string, body any) (*SecurityCreateResponse, error)
DeleteRole(ctx, name string) (*types.AcknowledgedResponse, error)
GetRoleMapping(ctx, names []string) (map[string]*SecurityRoleMapping, error)
PutRoleMapping(ctx, name string, body any) (*SecurityRoleMappingCreateResponse, error)
DeleteRoleMapping(ctx, name string) (*types.AcknowledgedResponse, error)
// SecurityUser { Username string; Roles []string; FullName string `json:"full_name,omitempty"`; Email string; Metadata map[string]any; Enabled bool }
// SecurityRole { Cluster []string; Indices []SecurityRoleIndex; Applications []SecurityRoleApplication; RunAs []string; Metadata map[string]any; TransientMetadata map[string]any }  // ⚠ no `global` field
// SecurityRoleMapping { Enabled bool; Roles []string; RoleTemplates []map[string]any; Rules map[string]any; Metadata map[string]any }

// Cluster (api.ClusterService)
Health(ctx, indices []string, params *ClusterHealthParams) (*ClusterHealthResponse, error)
PutSettings(ctx, body any, params *ClusterPutSettingsParams) (*ClusterPutSettingsResponse, error)
GetComponentTemplate(ctx, names []string, params *ClusterGetComponentTemplateParams) (*ClusterGetComponentTemplateResponse, error)
PutComponentTemplate(ctx, req *PutComponentTemplateRequest) (*types.AcknowledgedResponse, error) // req{Name string; Body any; Params}
DeleteComponentTemplate(ctx, name string, params *ClusterDeleteComponentTemplateParams) (*types.AcknowledgedResponse, error)
// ClusterHealthResponse { ClusterName string; Status string; TimedOut bool; NumberOfNodes int; ... }
// ⚠ ClusterGetComponentTemplateResponse.ComponentTemplates[].ComponentTemplate is typed `TemplateBody` DIRECTLY
//   (missing the nested `template` object + `version`/`_meta`) → use raw GET (see §7.9).

// Indices (api.IndicesService) — composable index templates
GetIndexTemplate(ctx, names []string, params *IndicesGetIndexTemplateParams) (*IndicesGetIndexTemplateResponse, error)
PutIndexTemplate(ctx, req *PutIndexTemplateRequest) (*types.AcknowledgedResponse, error) // req{Name string; Body any; Params}
DeleteIndexTemplate(ctx, name string, params *IndicesDeleteIndexTemplateParams) (*types.AcknowledgedResponse, error)
// IndicesGetIndexTemplateResponse { IndexTemplates []IndexTemplateItem{Name string; IndexTemplate IndexTemplate `json:"index_template"`} }
// IndexTemplate { IndexPatterns []string; Template TemplateBody `json:"template"`; Priority *int; Version *int; ComposedOf []string; DataStream map[string]any; AllowAutoCreate *bool }  // ⚠ missing `_meta`

// Ingest (api.IngestService)
GetPipeline(ctx, ids []string, params *IngestGetPipelineParams) (map[string]*IngestPipeline, error)
PutPipeline(ctx, req *IngestPutPipelineRequest) (*types.AcknowledgedResponse, error) // req{Id string; Body any; Params}
DeletePipeline(ctx, id string, params *IngestDeletePipelineParams) (*types.AcknowledgedResponse, error)
// IngestPipeline { Description string; Version int; Processors []map[string]any; OnFailure []map[string]any; Deprecated bool }

// Watcher (api.WatcherService)
GetWatch(ctx, id string) (*WatcherGetWatchResponse, error)
PutWatch(ctx, id string, body any, params *WatcherPutWatchParams) (*WatcherPutWatchResponse, error)
DeleteWatch(ctx, id string) (*WatcherDeleteWatchResponse, error)
// WatcherGetWatchResponse { Found bool; ID string; Status map[string]any; Watch map[string]any `json:"watch,omitempty"` }

// Transform (api.TransformService)
Get(ctx, transformIds []string, params *TransformGetParams) (*TransformGetResponse, error)
Put(ctx, transformId string, body any, params *TransformPutParams) (*TransformPutResponse, error)
Delete(ctx, transformId string, params *TransformDeleteParams) (*types.AcknowledgedResponse, error)
// TransformGetResponse { Count int; Transforms []TransformConfig }
// TransformConfig { ID string; Description string; Source,Dest,Sync,Pivot,Latest,Settings,Meta map[string]any; Frequency string; Version *int; CreateTime int64 }  // ⚠ missing `retention_policy`

// License (api.LicenseService)
Get(ctx, params *LicenseGetParams) (*LicenseGetResponse, error)          // LicenseGetResponse { License LicenseInfo }
Delete(ctx) (*types.AcknowledgedResponse, error)
Post(ctx, body any, params *LicensePostParams) (*types.AcknowledgedResponse, error)  // PUT /_license
GetBasicStatus(ctx) (json.RawMessage, error)
PostStartBasic(ctx, params *LicensePostParams) (json.RawMessage, error)
// LicenseInfo { UID string `json:"uid"`; Type string `json:"type"`; Status string; ... }
// LicensePostParams { Common *CommonParams; Acknowledge *bool }
```

## 3. go.mod changes

```go
module github.com/disaster37/es-handler/v9
go 1.26
```

- **Remove** direct: `github.com/elastic/go-elasticsearch/v8`, `github.com/olivere/elastic/v7`.
- **Add** direct: `github.com/disaster37/elasticsearch/v9` (pulls resty/v2, goccy/go-json, validator/v10, otel, …).
- **Keep** direct: `github.com/disaster37/generic-objectmatcher`, `github.com/elastic/go-ucfg`, `github.com/google/go-cmp`, `github.com/jarcoal/httpmock`, `github.com/json-iterator/go`, `github.com/pkg/errors`, `github.com/sirupsen/logrus`, `github.com/stretchr/testify`, `go.uber.org/mock`.
- **Indirect deps that drop** (olivere/elastic only): `github.com/elastic/elastic-transport-go/v8`, `github.com/mailru/easyjson`, `github.com/josharian/intern`, likely more. Let `go mod tidy` decide.
- **`github.com/disaster37/k8s-objectmatcher` stays indirect** (still required by `generic-objectmatcher`). Do not remove manually.

Commands (implementer):

```sh
cd /projects/es-handler
go mod edit -module github.com/disaster37/es-handler/v9
go mod edit -go=1.26
go get github.com/disaster37/elasticsearch/v9@v9.0.0   # confirm exact tag/pseudo-version: go list -m -versions github.com/disaster37/elasticsearch/v9
go mod tidy   # after source edits
```

Update import path `github.com/disaster37/es-handler/v8/patch` → `github.com/disaster37/es-handler/v9/patch` in `component_template.go` and `index_template.go`.

## 4. Type mapping table (public interface)

| Resource | Old type | New type |
|---|---|---|
| `Client()` | `*elastic.Client` | `elasticsearch.Client` (interface) |
| License Get/Diff | `*olivere.XPackInfoLicense` | `*esapi.LicenseInfo` |
| ILM | `*olivere.XPackIlmGetLifecycleResponse` | `*esapi.IlmPolicy` |
| SLM | `*SnapshotLifecyclePolicySpec` (local) | **KEEP** |
| SnapshotRepository | `*olivere.SnapshotRepositoryMetaData` | `*esapi.SnapshotRepository` |
| Role | `*XPackSecurityRole` (local) | **KEEP** |
| RoleMapping | `*olivere.XPackSecurityRoleMapping` | `*esapi.SecurityRoleMapping` |
| User create/update/diff | `*olivere.XPackSecurityPutUserRequest` | `*SecurityPutUserRequest` (**new**) |
| User get | `*olivere.XPackSecurityUser` | `*esapi.SecurityUser` |
| ComponentTemplate | `*olivere.IndicesGetComponentTemplate` | `*localpatch.ComponentTemplate` (**new**) |
| IndexTemplate | `*olivere.IndicesGetIndexTemplate` | `*localpatch.IndexTemplate` (**new**) |
| Watch | `*olivere.XPackWatch` | `*XPackWatch` (**new alias** `= map[string]any`) |
| IngestPipeline | `*olivere.IngestGetPipeline` | `*esapi.IngestPipeline` |
| Transform | `*Transform` (local) | **KEEP** |
| ClusterHealth | `*olivere.ClusterHealthResponse` | `*esapi.ClusterHealthResponse` |

Local types **KEPT unchanged** (already olivere-free): `XPackSecurityRole` + `XPackSecurityApplicationPrivileges` + `XPackSecurityIndicesPermissions` (role.go); `SnapshotLifecyclePolicy`, `SnapshotLifecyclePolicySpec`, `ElasticsearchSLMConfig`, `ElasticsearchSLMRetention`, `SnapshotLifecyclePolicyGet` (slm.go); `Transform` + `TransformSource/Dest/Pivot/Latest/Sync/Retention/SyncTime/RetentionTime` + `TransformGetResponse` (transform.go).

## 5. `elasticsearch.go` — struct, constructor, `Client()`, interface

Imports:

```go
import (
	"github.com/disaster37/generic-objectmatcher/patch"
	elasticsearch "github.com/disaster37/elasticsearch/v9"
	esapi "github.com/disaster37/elasticsearch/v9/api"
	localpatch "github.com/disaster37/es-handler/v9/patch"
	"github.com/sirupsen/logrus"
)
```

```go
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

func (h *ElasticsearchHandlerImpl) SetLogger(log *logrus.Entry) { h.log = log }
func (h *ElasticsearchHandlerImpl) Client() elasticsearch.Client { return h.client }
```

Full interface (only types change):

```go
type ElasticsearchHandler interface {
	Client() (client elasticsearch.Client)

	LicenseUpdate(license string) (err error)
	LicenseDelete() (err error)
	LicenseGet() (license *esapi.LicenseInfo, err error)
	LicenseDiff(actual, expected *esapi.LicenseInfo) (diff bool)
	LicenseEnableBasic() (err error)

	ILMUpdate(name string, policy *esapi.IlmPolicy) (err error)
	ILMDelete(name string) (err error)
	ILMGet(name string) (policy *esapi.IlmPolicy, err error)
	ILMDiff(actualObject, expectedObject, originalObject *esapi.IlmPolicy) (patchResult *patch.PatchResult, err error)

	SLMUpdate(name string, policy *SnapshotLifecyclePolicySpec) (err error)
	SLMDelete(name string) (err error)
	SLMGet(name string) (policy *SnapshotLifecyclePolicySpec, err error)
	SLMDiff(actualObject, expectedObject, originalObject *SnapshotLifecyclePolicySpec) (patchResult *patch.PatchResult, err error)

	SnapshotRepositoryUpdate(name string, repository *esapi.SnapshotRepository) (err error)
	SnapshotRepositoryDelete(name string) (err error)
	SnapshotRepositoryGet(name string) (repository *esapi.SnapshotRepository, err error)
	SnapshotRepositoryDiff(actualObject, expectedObject, originalObject *esapi.SnapshotRepository) (patchResult *patch.PatchResult, err error)

	RoleUpdate(name string, role *XPackSecurityRole) (err error)
	RoleDelete(name string) (err error)
	RoleGet(name string) (role *XPackSecurityRole, err error)
	RoleDiff(actualObject, expectedObject, originalObject *XPackSecurityRole) (patchResult *patch.PatchResult, err error)

	RoleMappingUpdate(name string, roleMapping *esapi.SecurityRoleMapping) (err error)
	RoleMappingDelete(name string) (err error)
	RoleMappingGet(name string) (roleMapping *esapi.SecurityRoleMapping, err error)
	RoleMappingDiff(actualObject, expectedObject, originalObject *esapi.SecurityRoleMapping) (patchResult *patch.PatchResult, err error)

	UserCreate(name string, user *SecurityPutUserRequest) (err error)
	UserUpdate(name string, user *SecurityPutUserRequest, isProtected ...bool) (err error)
	UserDelete(name string) (err error)
	UserGet(name string) (user *esapi.SecurityUser, err error)
	UserDiff(actualObject, expectedObject, originalObject *SecurityPutUserRequest) (patchResult *patch.PatchResult, err error)

	ComponentTemplateUpdate(name string, component *localpatch.ComponentTemplate) (err error)
	ComponentTemplateDelete(name string) (err error)
	ComponentTemplateGet(name string) (component *localpatch.ComponentTemplate, err error)
	ComponentTemplateDiff(actualObject, expectedObject, originalObject *localpatch.ComponentTemplate) (patchResult *patch.PatchResult, err error)

	IndexTemplateUpdate(name string, template *localpatch.IndexTemplate) (err error)
	IndexTemplateDelete(name string) (err error)
	IndexTemplateGet(name string) (template *localpatch.IndexTemplate, err error)
	IndexTemplateDiff(actualObject, expectedObject, originalObject *localpatch.IndexTemplate) (patchResult *patch.PatchResult, err error)

	WatchUpdate(name string, watch *XPackWatch) (err error)
	WatchDelete(name string) (err error)
	WatchGet(name string) (watch *XPackWatch, err error)
	WatchDiff(actualObject, expectedObject, originalObject *XPackWatch) (patchResult *patch.PatchResult, err error)

	IngestPipelineUpdate(name string, pipeline *esapi.IngestPipeline) (err error)
	IngestPipelineDelete(name string) (err error)
	IngestPipelineGet(name string) (pipeline *esapi.IngestPipeline, err error)
	IngestPipelineDiff(actualObject, expectedObject, originalObject *esapi.IngestPipeline) (patchResult *patch.PatchResult, err error)

	TransformUpdate(name string, transform *Transform) (err error)
	TransformDelete(name string) (err error)
	TransformGet(name string) (transform *Transform, err error)
	TransformDiff(actualObject, expectedObject, originalObject *Transform) (patchResult *patch.PatchResult, err error)

	ClusterHealth() (health *esapi.ClusterHealthResponse, err error)
	EnableRoutingRebalance() (err error)
	DisableRoutingRebalance() (err error)
	EnableRoutingAllocation() (err error)
	DisableRoutingAllocation() (err error)

	SetLogger(log *logrus.Entry)
}
```

> **Decision (recommended):** expose the new `elasticsearch.Client` interface from `Client()`. Low-level access via `Client().RestyClient()`; mocks continue to mock `ElasticsearchHandler`.

## 6. Error-handling strategy

| Old pattern | New pattern |
|---|---|
| `if err != nil { return nil, err }` | unchanged (network errors returned as-is) |
| `if res.IsError() { if res.StatusCode == 404 {…} … }` | `if err != nil { if elasticsearch.IsNotFound(err) {…}; … }` |
| `errors.Errorf("Error when …: %s", res.String())` | `errors.Wrapf(err, "Error when …")` (`*types.ElasticsearchError.Error()` already includes status+reason) |

- **Delete** missing resource → `IsNotFound(err)` → log + `return nil`.
- **Get** missing resource → `IsNotFound(err)` → `return nil, nil`.
- `IsConflict` (409) / `IsUnauthorized` (401) available for new tests; not branched in prod code today.

Import `elasticsearch "github.com/disaster37/elasticsearch/v9"` in every resource file for `IsNotFound`.

## 7. Per-resource rewrite

All keep `context.Background()` where old code used it. All `*Diff` keep the existing `if actualObject == nil { …jsonIterator… } else { patch.DefaultPatchMaker.Calculate(...) }` shape — only parameter types change.

### 7.1 `cluster.go`
```go
func (h *ElasticsearchHandlerImpl) ClusterHealth() (*esapi.ClusterHealthResponse, error) {
	return h.client.Cluster().Health(context.Background(), nil, nil)
}
func (h *ElasticsearchHandlerImpl) EnableRoutingRebalance() error { // and Disable/Allocation variants
	settings := map[string]interface{}{"persistent": map[string]interface{}{"cluster.routing.rebalance.enable": "all"}}
	_, err := h.client.Cluster().PutSettings(context.Background(), settings, nil)
	return err
}
```
DisableRebalance → `"none"`; EnableAllocation → `"all"`; DisableAllocation → `"primaries"`. Drop `bytes`/`io`/`encoding/json`/`olivere`; add `esapi`, `context`.

### 7.2 `license.go`
- `LicenseEnableBasic`: `data, err := h.client.License().GetBasicStatus(context.Background())` → unmarshal `data` (json.RawMessage) into `map[string]interface{}`; if `eligible_to_start_basic == false` log+return nil; else `ack := true; _, err = h.client.License().PostStartBasic(context.Background(), &esapi.LicensePostParams{Acknowledge: &ack})`.
- `LicenseUpdate(license string)`: `ack := true; _, err := h.client.License().Post(context.Background(), license, &esapi.LicensePostParams{Acknowledge: &ack})` (string body sent verbatim).
- `LicenseDelete`: `_, err := h.client.License().Delete(context.Background())`; on `IsNotFound` log "License not found, skip it" + nil.
- `LicenseGet`: `resp, err := h.client.License().Get(context.Background(), nil)`; on `IsNotFound` return nil,nil; else `return &resp.License, nil`.
- `LicenseDiff`: unchanged logic; type `*esapi.LicenseInfo` (fields `.Type`, `.UID`).

### 7.3 `ilm.go`
```go
ILMUpdate:  _, err := h.client.ILM().PutLifecycle(context.Background(), name, policy)
ILMDelete:  _, err := h.client.ILM().DeleteLifecycle(context.Background(), name); if IsNotFound → nil
ILMGet:     policies, err := h.client.ILM().GetLifecycle(context.Background(), []string{name}); if IsNotFound → nil,nil; return policies[name]
```
`ILMDiff` unchanged with `*esapi.IlmPolicy`. Drop `bytes`/`io`/`olivere`; add `esapi` + `elasticsearch`.

### 7.4 `slm.go` (KEEP local types; raw GET)
```go
SLMUpdate:  _, err := h.client.SLM().PutLifecycle(context.Background(), name, policy)   // policy *SnapshotLifecyclePolicySpec
SLMDelete:  _, err := h.client.SLM().DeleteLifecycle(context.Background(), name); if IsNotFound → nil
SLMGet:     b, err := h.getRaw("/_slm/policy/" + url.PathEscape(name))
            if err != nil || b == nil { return nil, err }
            slm := make(SnapshotLifecyclePolicy)   // map[string]*SnapshotLifecyclePolicyGet
            json.Unmarshal(b, &slm)
            if len(slm) == 0 { return nil, nil }   // ES bug #47664
            return slm[name].Policy, nil
```
`SLMDiff` unchanged. Add `net/url`, `elasticsearch`; keep `encoding/json`.

### 7.5 `snapshot_repositoty.go`
```go
SnapshotRepositoryUpdate:  _, err := h.client.Snapshot().CreateRepository(context.Background(), name, repository, nil)
SnapshotRepositoryDelete:  _, err := h.client.Snapshot().DeleteRepository(context.Background(), []string{name}, nil); if IsNotFound → nil
SnapshotRepositoryGet:     repos, err := h.client.Snapshot().GetRepository(context.Background(), []string{name}, nil); if IsNotFound → nil,nil; return repos[name]
```
`SnapshotRepositoryDiff` unchanged with `*esapi.SnapshotRepository`. Note `Settings` is now `map[string]string`.

### 7.6 `role.go` (KEEP local types; raw GET to preserve `global`)
```go
RoleUpdate:  _, err := h.client.Security().PutRole(context.Background(), name, role); wrap err
RoleDelete:  _, err := h.client.Security().DeleteRole(context.Background(), name); if IsNotFound → nil; log "Deleted role"
RoleGet:     b, err := h.getRaw("/_security/role/" + url.PathEscape(name))
             if err != nil || b == nil { return nil, err }
             roleResp := make(map[string]XPackSecurityRole); json.Unmarshal(b, &roleResp)
             tmp := roleResp[name]; return &tmp, nil
```
`RoleDiff` unchanged. **Rationale:** `esapi.SecurityRole` drops `global` and changes `field_security`/`allow_restricted_indices` types (would change PUT bodies & Diff JSON). Raw GET preserves exact fidelity. (If `global` is provably unused, a later cleanup may adopt `esapi.SecurityRole`.)

### 7.7 `role_mapping.go`
```go
RoleMappingUpdate:  _, err := h.client.Security().PutRoleMapping(context.Background(), name, rm)
RoleMappingDelete:  _, err := h.client.Security().DeleteRoleMapping(context.Background(), name); if IsNotFound → nil
RoleMappingGet:     mappings, err := h.client.Security().GetRoleMapping(context.Background(), []string{name}); if IsNotFound → nil,nil; return mappings[name]
```
`RoleMappingDiff` unchanged with `*esapi.SecurityRoleMapping`.

### 7.8 `user.go` (new local request type)
```go
type SecurityPutUserRequest struct {
	Enabled      bool           `json:"enabled,omitempty"`
	Email        string         `json:"email,omitempty"`
	FullName     string         `json:"full_name,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	Password     string         `json:"password,omitempty"`
	PasswordHash string         `json:"password_hash,omitempty"`
	Roles        []string       `json:"roles,omitempty"`
}
```
```go
UserCreate:  _, err := h.client.Security().PutUser(context.Background(), name, user)
UserUpdate:  isP := len(isProtected) > 0 && isProtected[0]
             if user.Password != "" || user.PasswordHash != "" {
                 payload := map[string]string{...}   // "password" or "password_hash"
                 if _, err := h.client.Security().ChangePassword(context.Background(), name, payload); err != nil { return wrap }
             }
             if isP { return nil }
             user.Password, user.PasswordHash = "", ""
             return h.UserCreate(name, user)
UserDelete:  _, err := h.client.Security().DeleteUser(context.Background(), name); if IsNotFound → nil
UserGet:     users, err := h.client.Security().GetUser(context.Background(), []string{name}); if IsNotFound → nil,nil; return users[name]
```
`UserDiff` unchanged with `*SecurityPutUserRequest`.
> ⚠ `ChangePassword` is **POST** `/_security/user/{u}/_password` — fix the old test's `PUT` responder to `POST` (see §10).

### 7.9 `component_template.go` (raw GET; typed PUT/DELETE)
```go
ComponentTemplateUpdate:  req := &esapi.PutComponentTemplateRequest{Name: name, Body: component}
                          _, err := h.client.Cluster().PutComponentTemplate(context.Background(), req)
ComponentTemplateDelete:  _, err := h.client.Cluster().DeleteComponentTemplate(context.Background(), name, nil); if IsNotFound → nil
ComponentTemplateGet:     b, err := h.getRaw("/_component_template/" + url.PathEscape(name))
                          if err != nil || b == nil { return nil, err }
                          resp := &localpatch.ComponentTemplateGetResponse{}; json.Unmarshal(b, resp)
                          if len(resp.ComponentTemplates) == 0 { return nil, nil }
                          return resp.ComponentTemplates[0].ComponentTemplate, nil
```
`ComponentTemplateDiff` unchanged; pass `localpatch.ConvertComponentTemplateSetting`.

### 7.10 `index_template.go` (raw GET; typed PUT/DELETE)
```go
IndexTemplateUpdate:  req := &esapi.PutIndexTemplateRequest{Name: name, Body: template}
                      _, err := h.client.Indices().PutIndexTemplate(context.Background(), req)
IndexTemplateDelete:  _, err := h.client.Indices().DeleteIndexTemplate(context.Background(), name, nil); if IsNotFound → nil
IndexTemplateGet:     b, err := h.getRaw("/_index_template/" + url.PathEscape(name))
                      if err != nil || b == nil { return nil, err }
                      resp := &localpatch.IndexTemplateGetResponse{}; json.Unmarshal(b, resp)
                      if len(resp.IndexTemplates) == 0 { return nil, nil }
                      return resp.IndexTemplates[0].IndexTemplate, nil
```
`IndexTemplateDiff` unchanged; pass `localpatch.ConvertIndexTemplateSetting`.

### 7.11 `watch.go` (new local alias)
```go
type XPackWatch = map[string]any   // alias so *XPackWatch == *map[string]any

WatchUpdate:  _, err := h.client.Watcher().PutWatch(context.Background(), name, watch, nil)
WatchDelete:  _, err := h.client.Watcher().DeleteWatch(context.Background(), name); if IsNotFound → nil
WatchGet:     resp, err := h.client.Watcher().GetWatch(context.Background(), name)
              if IsNotFound → nil,nil; if resp == nil || !resp.Found → nil,nil
              return &resp.Watch, nil
```
`WatchDiff` unchanged with `*XPackWatch`.

### 7.12 `ingest_pipeline.go`
```go
IngestPipelineUpdate:  req := &esapi.IngestPutPipelineRequest{Id: name, Body: pipeline}
                       _, err := h.client.Ingest().PutPipeline(context.Background(), req)
IngestPipelineDelete:  _, err := h.client.Ingest().DeletePipeline(context.Background(), name, nil); if IsNotFound → nil
IngestPipelineGet:     pipelines, err := h.client.Ingest().GetPipeline(context.Background(), []string{name}, nil); if IsNotFound → nil,nil; return pipelines[name]
```
`IngestPipelineDiff` unchanged with `*esapi.IngestPipeline`.

### 7.13 `transform.go` (KEEP local types; raw GET to preserve `retention_policy`)
```go
TransformUpdate:  _, err := h.client.Transform().Put(context.Background(), name, transform, nil)
TransformDelete:  _, err := h.client.Transform().Delete(context.Background(), name, nil); if IsNotFound → nil
TransformGet:     b, err := h.getRaw("/_transform/" + url.PathEscape(name))
                  if err != nil || b == nil { return nil, err }
                  transforms := &TransformGetResponse{}   // local type already exists
                  json.Unmarshal(b, transforms)
                  if len(transforms.Transforms) == 0 { return nil, nil }
                  return transforms.Transforms[0], nil
```
`TransformDiff` unchanged.

### 7.14 `raw.go` (NEW FILE; keep `helper.go` unchanged)
```go
package eshandler

import (
	"net/http"
	"github.com/pkg/errors"
)

// getRaw GETs through the underlying resty client; returns (nil, nil) on 404.
func (h *ElasticsearchHandlerImpl) getRaw(path string) ([]byte, error) {
	resp, err := h.client.RestyClient().R().Get(path)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() == http.StatusNotFound {
		return nil, nil
	}
	if resp.IsError() {
		return nil, errors.Errorf("Elasticsearch error %d: %s", resp.StatusCode(), string(resp.Body()))
	}
	return resp.Body(), nil
}
```

## 8. `patch/template_setting.go` rewrite

Replace olivere-embedding wrappers with standalone local types; simplify `walk` (no reflect):

```go
package patch

import (
	"fmt"
	json "github.com/json-iterator/go"
)

type ComponentTemplate struct {
	Template *ComponentTemplateData `json:"template"`
	Version  *int64                 `json:"version,omitempty"`
	Meta     map[string]any         `json:"_meta,omitempty"`
}
type ComponentTemplateData struct {
	Settings map[string]any `json:"settings,omitempty"`
	Mappings map[string]any `json:"mappings,omitempty"`
	Aliases  map[string]any `json:"aliases,omitempty"`
}
type ComponentTemplateGetResponse struct {
	ComponentTemplates []struct {
		Name              string             `json:"name"`
		ComponentTemplate *ComponentTemplate `json:"component_template"`
	} `json:"component_templates"`
}

type IndexTemplate struct {
	IndexPatterns []string           `json:"index_patterns"`
	Template      *IndexTemplateData `json:"template"`
	Priority      int                `json:"priority,omitempty"`
	Version       int                `json:"version,omitempty"`
	ComposedOf    []string           `json:"composed_of,omitempty"`
	DataStream    map[string]any     `json:"data_stream,omitempty"`
	Meta          map[string]any     `json:"_meta,omitempty"`
}
type IndexTemplateData struct {
	Settings map[string]any `json:"settings,omitempty"`
	Mappings map[string]any `json:"mappings,omitempty"`
	Aliases  map[string]any `json:"aliases,omitempty"`
}
type IndexTemplateGetResponse struct {
	IndexTemplates []struct {
		Name          string         `json:"name"`
		IndexTemplate *IndexTemplate `json:"index_template"`
	} `json:"index_templates"`
}

// walk converts every float64 in nested maps/slices to a decimal string.
func walk(v any) any {
	switch v := v.(type) {
	case []any:
		for i, c := range v { v[i] = walk(c) }
		return v
	case map[string]any:
		for k, c := range v { v[k] = walk(c) }
		return v
	case float64:
		return fmt.Sprintf("%d", int64(v))
	default:
		return v
	}
}

func ConvertComponentTemplateSetting(actualByte, expectedByte []byte) ([]byte, []byte, error) {
	actual := &ComponentTemplate{}
	expected := &ComponentTemplate{}
	if err := json.ConfigCompatibleWithStandardLibrary.Unmarshal(actualByte, actual); err != nil { return nil, nil, err }
	if err := json.ConfigCompatibleWithStandardLibrary.Unmarshal(expectedByte, expected); err != nil { return nil, nil, err }
	if actual.Template != nil && actual.Template.Settings != nil {
		actual.Template.Settings = walk(actual.Template.Settings).(map[string]any)
	}
	if expected.Template != nil && expected.Template.Settings != nil {
		expected.Template.Settings = walk(expected.Template.Settings).(map[string]any)
	}
	actualByte, err := json.ConfigCompatibleWithStandardLibrary.Marshal(actual); if err != nil { return nil, nil, err }
	expectedByte, err = json.ConfigCompatibleWithStandardLibrary.Marshal(expected); if err != nil { return nil, nil, err }
	return actualByte, expectedByte, nil
}

// ConvertIndexTemplateSetting: same but with IndexTemplate + IndexTemplateData.
```
Remove the old `IndicesGetComponentTemplate`/`IndicesGetIndexTemplate` embedding wrappers and the reflect-based `walk`.

## 9. Test migration

### 9.1 `suite_test.go` (full rewrite of SetupTest)

```go
package eshandler

import (
	"net/http"
	"testing"

	elasticsearch "github.com/disaster37/elasticsearch/v9"
	"github.com/jarcoal/httpmock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

const baseURL = "http://localhost:9200"

type ElasticsearchHandlerTestSuite struct {
	suite.Suite
	esHandler ElasticsearchHandler
}

func TestElasticsearchHandlerSuite(t *testing.T) { suite.Run(t, new(ElasticsearchHandlerTestSuite)) }

func (t *ElasticsearchHandlerTestSuite) SetupTest() {
	// Order is critical: elasticsearch.New clones http.DefaultTransport with a
	// hard *http.Transport assertion, so New MUST run BEFORE httpmock.Activate().
	cfg := &elasticsearch.Config{URL: baseURL}
	client, err := elasticsearch.New(cfg, logrus.NewEntry(logrus.New()))
	if err != nil {
		panic(err)
	}
	// Point the underlying resty client at httpmock's transport AFTER New().
	// httpmock.DefaultTransport (*httpmock.MockTransport) implements http.RoundTripper.
	client.RestyClient().SetTransport(httpmock.DefaultTransport)

	t.esHandler = &ElasticsearchHandlerImpl{
		client: client,
		log:    logrus.NewEntry(logrus.New()),
	}

	httpmock.Activate() // now safe: http.DefaultTransport is no longer used by our client
}

func (t *ElasticsearchHandlerTestSuite) BeforeTest(suiteName, testName string) {
	httpmock.Reset()
}

func SetHeaders(resp *http.Response) { // optional now (X-Elastic-Product is only a DEBUG log)
	resp.Header.Add("X-Elastic-Product", "Elasticsearch")
}
```

Key facts:
- `elasticsearch.New` panics if `httpmock.Activate()` already ran (its `*http.Transport` assertion). So `New` → `SetTransport(httpmock.DefaultTransport)` → `httpmock.Activate()`.
- `httpmock.DefaultTransport` is `*httpmock.MockTransport`, assignable to `http.RoundTripper` (it implements `RoundTrip`).
- `Config` has no `Transport`/`Addresses`; use `URL: baseURL`.
- Raw GET paths (e.g. `/_slm/policy/test`) are relative and resolved against resty's `SetBaseURL(baseURL)`.

### 9.2 Per-test-file type substitutions

| Test file | Changes |
|---|---|
| `cluster_test.go` | `olivere.ClusterHealthResponse` → `esapi.ClusterHealthResponse`; remove `olivere` import; add `esapi`. `rawHealth` JSON unchanged (field names match). |
| `license_test.go` | `olivere.XPackInfoServiceResponse`/`olivere.XPackInfoLicense` → build `esapi.LicenseGetResponse{License: esapi.LicenseInfo{...}}` (or return raw `{"license":{"uid":..,"type":..}}` JSON string). `LicenseDiff` test types → `*esapi.LicenseInfo`. |
| `ilm_test.go` | `olivere.XPackIlmGetLifecycleResponse` → `esapi.IlmPolicy`; `map[string]*olivere.XPackIlmGetLifecycleResponse` → `map[string]*esapi.IlmPolicy`. Remove `olivere`. |
| `slm_test.go` | No type change (already local types). Test unchanged; add 404 case. |
| `snapshot_repository_test.go` | `olivere.SnapshotRepositoryMetaData` → `esapi.SnapshotRepository`; `olivere.SnapshotGetRepositoryResponse` → `map[string]*esapi.SnapshotRepository`; `Settings` values become `map[string]string`. |
| `role_test.go` | No type change (local `XPackSecurityRole`). Test unchanged; add 404 case. |
| `role_mapping_test.go` | `olivere.XPackSecurityRoleMapping` → `esapi.SecurityRoleMapping`; `olivere.XPackSecurityGetRoleMappingResponse` → `map[string]*esapi.SecurityRoleMapping`. |
| `user_test.go` | `olivere.XPackSecurityPutUserRequest` → `SecurityPutUserRequest`; `olivere.XPackSecurityUser` → `esapi.SecurityUser`; `olivere.XPackSecurityGetUserResponse` → `map[string]*esapi.SecurityUser`; rename `Fullname` → `FullName`; change the change-password responder from `PUT` to **`POST`** at `urlUser + "/_password"`. |
| `component_template_test.go` | `olivere.IndicesGetComponentTemplate` → `localpatch.ComponentTemplate`; `olivere.IndicesGetComponentTemplateData` → `localpatch.ComponentTemplateData`; `olivere.IndicesGetComponentTemplateResponse`/`IndicesGetComponentTemplates` → build `localpatch.ComponentTemplateGetResponse` (or raw JSON). |
| `index_template_test.go` | `olivere.IndicesGetIndexTemplate` → `localpatch.IndexTemplate`; `olivere.IndicesGetIndexTemplateData` → `localpatch.IndexTemplateData`; `olivere.IndicesGetIndexTemplateResponse`/`IndicesGetIndexTemplates`/`IndicesGetIndexTemplatesSlice` → `localpatch.IndexTemplateGetResponse`. |
| `watch_test.go` | `olivere.XPackWatch` → `XPackWatch` (alias `map[string]any`). `&olivere.XPackWatch{}` → `&XPackWatch{}`. Remove `olivere`. |
| `ingest_pipeline_test.go` | `olivere.IngestGetPipeline` → `esapi.IngestPipeline`; `olivere.IngestGetPipelineResponse` → `map[string]*esapi.IngestPipeline`. |
| `transform_test.go` | No type change (local `Transform`, `TransformGetResponse`). Test unchanged; add 404 case. |
| `patch/template_setting_test.go` | Replace `elastic.IndicesGetComponentTemplate`/`elastic.IndicesGetIndexTemplate` + `IndicesGetComponentTemplateData`/`IndicesGetIndexTemplateData` with `patch.ComponentTemplate`/`patch.IndexTemplate` + `patch.ComponentTemplateData`/`patch.IndexTemplateData`. Remove `github.com/olivere/elastic/v7` import. |

### 9.3 Enhanced coverage to ADD

Current tests only cover success + network-error. Add:

1. **404 not-found for every Get** (returns `nil, nil`) and **every Delete** (returns `nil`) — register a responder returning HTTP 404 with an ES error body `{"error":{"type":"resource_not_found_exception","reason":"..."},"status":404}`.
2. **401 unauthorized** (register 401) → assert `err != nil` and `elasticsearch.IsUnauthorized(err)` true (license, cluster, security get paths).
3. **409 conflict** on create/update → assert `err != nil` and `elasticsearch.IsConflict(err)` true.
4. **Body serialization assertions**: for each `*Update`/`Create`/`PutSettings`, register a responder that reads `req.Body`, and assert the JSON matches the expected payload (e.g. ILMUpdate sends `{"policy":{...}}`; RoleUpdate sends `{"cluster":["all"],"indices":[...]}`; LicenseUpdate sends the raw license string verbatim; PutSettings sends `{"persistent":{"cluster.routing.rebalance.enable":"all"}}`).
5. **Empty-name validation**: new services return `"name is required"` (a plain error, not `*ElasticsearchError`) for empty names — add tests asserting error for empty name on Update/Delete/Get where applicable, and confirm behavior matches expectations.

## 10. Mock regeneration

After the interface is finalized, regenerate `mocks/elasticsearch_handler.go`:

```sh
go install go.uber.org/mock/mockgen@v0.3.0
mockgen --build_flags=--mod=mod -destination=mocks/elasticsearch_handler.go -package=mocks github.com/disaster37/es-handler/v9 ElasticsearchHandler
```

Update `Makefile`:

```makefile
.PHONY: mock-gen
mock-gen:
	go install go.uber.org/mock/mockgen@v0.3.0
	mockgen --build_flags=--mod=mod -destination=mocks/elasticsearch_handler.go -package=mocks github.com/disaster37/es-handler/v9 ElasticsearchHandler
```

Expected changes in the generated mock: imports switch from `github.com/elastic/go-elasticsearch/v8` + `github.com/olivere/elastic/v7` to `github.com/disaster37/elasticsearch/v9` (for `Client`), `github.com/disaster37/elasticsearch/v9/api`, and `github.com/disaster37/es-handler/v9` (for `XPackSecurityRole`, `SnapshotLifecyclePolicySpec`, `Transform`, `SecurityPutUserRequest`), plus `github.com/disaster37/es-handler/v9/patch` (for `ComponentTemplate`/`IndexTemplate`).

## 11. Acceptance tests (Elasticsearch 9.x)

- **There is no in-repo ES version pin**: no `.github/`, no `ci/`, no `docker-compose*.yml`, no `Dockerfile`, no `*.yml`/`*.yaml`/`*.sh` in the repo (verified). The only config file is `.theia/launch.json` (debug config, no ES version).
- Therefore "use Elasticsearch v9 on acc test" is an **external** concern: the downstream acceptance-test harness (Terraform provider / Crossplane / operator repo) must run against an Elasticsearch **9.x** cluster. No files in THIS repo need changing for that.
- The public API type changes (§4) and the `NewElasticsearchHandler(cfg *elasticsearch.Config, …)` signature change must be propagated to all downstream consumers (they must import `github.com/disaster37/es-handler/v9` and construct `*elasticsearch.Config{URL: …}` instead of `elastic.Config{Addresses: …}`).

## 12. Edge cases, validation, error specifics

- **Empty names**: new service methods return `fmt.Errorf("name is required")` (a plain error, NOT `*types.ElasticsearchError`). `IsNotFound` will NOT match these. es-handler keeps its current pass-through behavior (no added validation). Test to lock this in.
- **Nil pointers** in `*Diff`: unchanged — `if actualObject == nil` path still returns a patch with `Patched: expectedObject`.
- **404 semantics**: Get → `(nil, nil)`; Delete → `nil` (idempotent). License Get/Delete log a warning on 404.
- **Body marshalling**: structs/maps passed as `body any` are marshaled by resty (encoding/json default) respecting JSON tags; `string`/`[]byte` bodies are sent verbatim (used by `LicenseUpdate`). If exact current serialization must be preserved (e.g. HTML escaping), pass `[]byte` from `json.Marshal(...)` instead of the struct.
- **Context**: all calls use `context.Background()` (matching old behavior). No cancellation/timeout added.
- **Watcher `Found`**: `GetWatch` returns 404 as `*ElasticsearchError` (doReq) → `IsNotFound`; additionally guard `resp == nil || !resp.Found` defensively.
- **`SnapshotRepository.Settings` is `map[string]string`** (was `map[string]interface{}`) — repository settings are strings in ES; adjust tests.
- **`SecurityUser.FullName`** (was `olivere.XPackSecurityUser.Fullname`) — field renamed; JSON key `full_name` unchanged.
- **`IngestPipeline.Version` is `int`** (old `olivere.IngestGetPipeline.Version` was `int64`); tests use int literals — fine.
- **`LicenseInfo` millis fields use camelCase tags** (`issue_dateInMillis` vs ES's `issue_date_in_millis`) — irrelevant to es-handler (only `.UID`/`.Type` are used), but do not rely on those millis fields.

## 13. Ordered implementation checklist

1. `go mod edit -module github.com/disaster37/es-handler/v9 && go mod edit -go=1.26`.
2. `go get github.com/disaster37/elasticsearch/v9@<resolved version>`.
3. `elasticsearch.go`: swap imports, `Client` type, `NewElasticsearchHandler`, `Client()`, full interface (per §5).
4. Add `raw.go` with `getRaw` (per §7.14).
5. Rewrite `patch/template_setting.go` (per §8).
6. Rewrite each resource file in order: `cluster.go`, `license.go`, `ilm.go`, `slm.go`, `snapshot_repositoty.go`, `role.go`, `role_mapping.go`, `user.go`, `component_template.go`, `index_template.go`, `watch.go`, `ingest_pipeline.go`, `transform.go` (per §7).
7. Update `Makefile` mock-gen target (per §10).
8. `go mod tidy` (cleans olivere/elastic transitive deps, adds new lib deps).
9. Rewrite `suite_test.go` SetupTest (per §9.1).
10. Update all `*_test.go` (per §9.2) + add enhanced coverage (per §9.3).
11. `gofmt -w .`.
12. Regenerate mocks: `make mock-gen` (per §10).
13. Verify:
    - `go build ./...`
    - `go vet ./...`
    - `go test ./...`
    - `go test ./... -run TestElasticsearchHandlerSuite -v` (spot-check)

## 14. Files to create / modify / delete

**Create:**
- `.opencode/plans/es9-migration.md` (this plan)
- `raw.go` (raw GET helper)

**Modify:**
- `go.mod`, `go.sum` (regenerated)
- `elasticsearch.go`
- `cluster.go`, `license.go`, `ilm.go`, `slm.go`, `snapshot_repositoty.go`, `role.go`, `role_mapping.go`, `user.go`, `component_template.go`, `index_template.go`, `watch.go`, `ingest_pipeline.go`, `transform.go`
- `patch/template_setting.go`
- `Makefile`
- `suite_test.go` and all 13 resource `*_test.go` + `patch/template_setting_test.go`
- `mocks/elasticsearch_handler.go` (regenerated)

**Delete:** none (no files removed; `helper.go` stays unchanged).

## 15. Open questions / risks (with recommendations)

1. **Role `global` field**: `esapi.SecurityRole` drops it. Recommendation: keep local `XPackSecurityRole` + raw GET (already decided). Confirm no consumer relies on `global`; if truly unused, a follow-up may adopt `esapi.SecurityRole`.
2. **`IndexTemplate` `_meta`, `TransformConfig` `retention_policy`, component-template nesting**: new lib models are incomplete/incorrect → kept local types + raw GET. Risk: if the v9 library later fixes these models, the raw-GET code can be simplified to typed calls.
3. **Go 1.26 toolchain**: mandatory. Confirm CI/dev machines have Go ≥ 1.26.
4. **Exact release of `github.com/disaster37/elasticsearch/v9`**: resolve the correct tag/pseudo-version via `go list -m -versions`; the plan assumes `v9.0.0`-equivalent.

