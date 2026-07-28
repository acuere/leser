// Package authz is the authorization core (order.md §6): granular permission
// constants, roles as permission sets, hierarchical scopes, and deny-by-
// default checks. Handlers never scatter if-statements; they declare a
// permission in the route table and the middleware enforces it.
package authz

// Permission is a granular capability, enumerated as constants — never
// free-form strings at check sites.
type Permission string

const (
	EventRead    Permission = "event:read"
	EventAdmin   Permission = "event:admin"
	IssueRead    Permission = "issue:read"
	IssueWrite   Permission = "issue:write"
	ProjectRead  Permission = "project:read"
	ProjectWrite Permission = "project:write"
	ProjectAdmin Permission = "project:admin"
	MemberRead   Permission = "member:read"
	MemberWrite  Permission = "member:write"
	TeamWrite    Permission = "team:write"
	OrgRead      Permission = "org:read"
	OrgWrite     Permission = "org:write"
	AlertsWrite  Permission = "alerts:write"
	TokenAdmin   Permission = "token:admin"
	AuditRead    Permission = "audit:read"
	OpsRead      Permission = "ops:read" // control-plane status/config
)

// AllPermissions enumerates every defined permission (route-table test uses
// this to reject unknown declarations).
var AllPermissions = []Permission{
	EventRead, EventAdmin, IssueRead, IssueWrite,
	ProjectRead, ProjectWrite, ProjectAdmin,
	MemberRead, MemberWrite, TeamWrite,
	OrgRead, OrgWrite, AlertsWrite, TokenAdmin, AuditRead, OpsRead,
}

// Role names. Custom roles are named permission sets stored in metadata;
// built-ins are defined here.
const (
	RoleOwner    = "owner"
	RoleManager  = "manager"
	RoleAdmin    = "admin"
	RoleMember   = "member"
	RoleBilling  = "billing"
	RoleReadOnly = "read_only"
)

// builtinRoles maps role name -> granted permissions.
var builtinRoles = map[string][]Permission{
	RoleOwner: {EventRead, EventAdmin, IssueRead, IssueWrite, ProjectRead, ProjectWrite, ProjectAdmin,
		MemberRead, MemberWrite, TeamWrite, OrgRead, OrgWrite, AlertsWrite, TokenAdmin, AuditRead, OpsRead},
	RoleManager: {EventRead, EventAdmin, IssueRead, IssueWrite, ProjectRead, ProjectWrite, ProjectAdmin,
		MemberRead, MemberWrite, TeamWrite, OrgRead, AlertsWrite, OpsRead},
	RoleAdmin: {EventRead, IssueRead, IssueWrite, ProjectRead, ProjectWrite,
		MemberRead, OrgRead, AlertsWrite, OpsRead},
	RoleMember:   {EventRead, IssueRead, IssueWrite, ProjectRead, OrgRead, OpsRead},
	RoleBilling:  {OrgRead},
	RoleReadOnly: {IssueRead, ProjectRead, OrgRead, OpsRead}, // note: no event:read — raw payloads (PII) denied
}

// RolePermissions returns the permission set for a built-in role (nil for
// unknown — deny by default).
func RolePermissions(role string) []Permission {
	return builtinRoles[role]
}

// Scope is the hierarchy org > team > project. A permission granted at org
// scope applies to every project in the org; project scope only to that
// project. Zero fields mean "not constrained at that level".
type Scope struct {
	OrgID     int64
	ProjectID int64
}

// AuthContext is a validated identity plus its grants. Only the auth
// middleware constructs one — this is the type-level gate: query helpers that
// need tenancy take an AuthContext, and there is no way to fabricate one
// without passing authentication.
type AuthContext struct {
	UserID    int64
	OrgID     int64
	Role      string
	perms     map[Permission]bool
	projectOK func(projectID int64) bool // object-level membership check
}

// NewAuthContext builds a context from a validated identity. projectOK
// verifies a project belongs to the identity's org (object-level check —
// route-level checks are not enough per order.md §6).
func NewAuthContext(userID, orgID int64, role string, extraPerms []Permission, projectOK func(int64) bool) *AuthContext {
	pm := map[Permission]bool{}
	for _, p := range RolePermissions(role) {
		pm[p] = true
	}
	for _, p := range extraPerms {
		pm[p] = true
	}
	return &AuthContext{UserID: userID, OrgID: orgID, Role: role, perms: pm, projectOK: projectOK}
}

// Restrict returns a copy whose permissions are the intersection with allowed
// — API tokens carry a strict subset of the granting user's permissions.
func (a *AuthContext) Restrict(allowed []Permission) *AuthContext {
	pm := map[Permission]bool{}
	for _, p := range allowed {
		if a.perms[p] {
			pm[p] = true
		}
	}
	return &AuthContext{UserID: a.UserID, OrgID: a.OrgID, Role: a.Role, perms: pm, projectOK: a.projectOK}
}

// Can reports whether the permission is granted. Deny by default: unknown
// permission or nil context is false.
func (a *AuthContext) Can(p Permission) bool {
	if a == nil {
		return false
	}
	return a.perms[p]
}

// CanProject reports permission within a project scope, enforcing the
// object-level tenancy check.
func (a *AuthContext) CanProject(p Permission, projectID int64) bool {
	if !a.Can(p) {
		return false
	}
	if a.projectOK == nil {
		return false // no membership oracle wired = deny
	}
	return a.projectOK(projectID)
}
