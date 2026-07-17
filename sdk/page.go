package sdk

import (
	"errors"
	"fmt"
)

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

func normalizePageRequest(request PageRequest) (PageRequest, error) {
	if request.Limit == 0 {
		request.Limit = DefaultPageSize
	}
	if request.Limit < 0 {
		return PageRequest{}, errors.New("page limit cannot be negative")
	}
	if request.Limit > MaxPageSize {
		return PageRequest{}, fmt.Errorf(
			"page limit %d exceeds maximum %d",
			request.Limit,
			MaxPageSize,
		)
	}
	return request, nil
}

func pageWindow[T any](
	items []T,
	request PageRequest,
	id func(T) string,
) ([]T, string, error) {
	normalized, err := normalizePageRequest(request)
	if err != nil {
		return nil, "", err
	}
	start := 0
	if normalized.After != "" {
		start = -1
		for index, item := range items {
			if id(item) == normalized.After {
				start = index + 1
				break
			}
		}
		if start < 0 {
			return nil, "", fmt.Errorf(
				"pagination cursor %q was not found",
				normalized.After,
			)
		}
	}
	end := min(start+normalized.Limit, len(items))
	page := append([]T(nil), items[start:end]...)
	next := ""
	if end < len(items) && len(page) > 0 {
		next = id(page[len(page)-1])
	}
	return page, next, nil
}

func PageWindow[T any](
	items []T,
	request PageRequest,
	id func(T) string,
) ([]T, string, error) {
	return pageWindow(items, request, id)
}
