package sdk

const (
	DefaultPageSize = 100
	MaxPageSize     = 1000
)

// PageRequest uses keyset pagination. After is the last item ID returned by a
// previous page; callers must not interpret it as an offset.
type PageRequest struct {
	After string `json:"after,omitempty"`
	Limit int    `json:"limit,omitempty"`
}
