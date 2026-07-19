package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lincyaw/ag/sdk"
	contextinjectionmodel "github.com/lincyaw/ag/sdk/storage/internal/contextinjectionmodel"
)

type memoryContextInjectionStore struct {
	mu           sync.Mutex
	injections   map[string]contextinjectionmodel.Record
	nextSequence uint64
}

func NewMemoryContextInjectionStore() sdk.ContextInjectionStore {
	return newMemoryContextInjectionStore()
}

func newMemoryContextInjectionStore() *memoryContextInjectionStore {
	return &memoryContextInjectionStore{
		injections: make(map[string]contextinjectionmodel.Record),
	}
}

func (store *memoryContextInjectionStore) Enqueue(
	ctx context.Context,
	injections ...sdk.ContextInjection,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, err := prepareContextInjections(injections, time.Now().UTC())
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, injection := range prepared {
		if existing, exists := store.injections[injection.ID]; exists {
			if sameContextInjectionIdentity(existing.Injection, injection) {
				continue
			}
			return fmt.Errorf(
				"context injection %q already exists with different identity",
				injection.ID,
			)
		}
	}
	for _, injection := range prepared {
		if _, exists := store.injections[injection.ID]; exists {
			continue
		}
		store.nextSequence++
		store.injections[injection.ID] = contextinjectionmodel.Record{
			Sequence:  store.nextSequence,
			Injection: sdk.CloneContextInjection(injection),
		}
	}
	return nil
}

func (store *memoryContextInjectionStore) List(
	ctx context.Context,
	query sdk.ContextInjectionQuery,
) ([]sdk.ContextInjection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateContextQuery(query); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	records := make(
		[]contextinjectionmodel.Record,
		0,
		len(store.injections),
	)
	for _, record := range store.injections {
		if !contextMatchesQuery(record.Injection, query) {
			continue
		}
		records = append(records, contextinjectionmodel.Record{
			Sequence:  record.Sequence,
			Injection: sdk.CloneContextInjection(record.Injection),
		})
	}
	sortContextRecords(records)
	if query.Limit > 0 && len(records) > query.Limit {
		records = records[:query.Limit]
	}
	result := make([]sdk.ContextInjection, len(records))
	for index, record := range records {
		result[index] = sdk.CloneContextInjection(record.Injection)
	}
	return result, nil
}

func (store *memoryContextInjectionStore) ConsumeContextInjections(
	ctx context.Context,
	ids ...string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateContextInjectionIDs(ids); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, id := range ids {
		delete(store.injections, id)
	}
	return nil
}
