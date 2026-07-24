// Package cagent marks the boundary of the frozen legacy terminal-agent model.
//
// Deprecated: new execution, session, event, tool, and persistence behavior
// belongs in sdk, sdk/runtime, or gateway. Packages below internal/cagent may be
// used by the legacy terminal presenter while it is being retired, but must not
// be imported by sdk, gateway, storage, transport, or plugin implementations.
package cagent
