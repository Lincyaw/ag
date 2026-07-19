package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/lincyaw/ag/internal/filestate"
	"github.com/lincyaw/ag/sdk"
	contextinjectionmodel "github.com/lincyaw/ag/sdk/storage/internal/contextinjectionmodel"
)

const contextInjectionStoreSchemaVersion uint32 = 1

type fileContextInjectionState struct {
	SchemaVersion uint32                                  `json:"schema_version"`
	NextSequence  uint64                                  `json:"next_sequence"`
	Injections    map[string]contextinjectionmodel.Record `json:"injections"`
}

type fileContextInjectionStore struct {
	directory string
	path      string
	lockPath  string
}

func NewFileContextInjectionStore(
	directory string,
) (sdk.ContextInjectionStore, error) {
	return newFileContextInjectionStore(directory, "context_injections.json")
}

func newFileContextInjectionStore(
	directory string,
	filename string,
) (*fileContextInjectionStore, error) {
	absolute, err := filestate.PrepareDirectory(
		"context injection",
		directory,
	)
	if err != nil {
		return nil, err
	}
	return &fileContextInjectionStore{
		directory: absolute,
		path:      filepath.Join(absolute, filename),
		lockPath:  filepath.Join(absolute, filename+".lock"),
	}, nil
}

func (store *fileContextInjectionStore) Enqueue(
	ctx context.Context,
	injections ...sdk.ContextInjection,
) error {
	return store.mutate(ctx, func(memory *memoryContextInjectionStore) error {
		return memory.Enqueue(ctx, injections...)
	})
}

func (store *fileContextInjectionStore) List(
	ctx context.Context,
	query sdk.ContextInjectionQuery,
) ([]sdk.ContextInjection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var result []sdk.ContextInjection
	err := filestate.WithSharedLock(store.lockPath, func() error {
		memory, readErr := store.readLocked()
		if readErr != nil {
			return readErr
		}
		result, readErr = memory.List(ctx, query)
		return readErr
	})
	return result, err
}

func (store *fileContextInjectionStore) mutate(
	ctx context.Context,
	mutation func(*memoryContextInjectionStore) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return filestate.WithExclusiveLock(store.lockPath, func() error {
		memory, err := store.readLocked()
		if err != nil {
			return err
		}
		if err := mutation(memory); err != nil {
			return err
		}
		return store.writeLocked(ctx, memory)
	})
}

func (store *fileContextInjectionStore) readLocked() (
	*memoryContextInjectionStore,
	error,
) {
	raw, err := os.ReadFile(store.path)
	if errors.Is(err, fs.ErrNotExist) {
		return newMemoryContextInjectionStore(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read context injections: %w", err)
	}
	var state fileContextInjectionState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("decode context injections: %w", err)
	}
	if state.SchemaVersion > contextInjectionStoreSchemaVersion {
		return nil, fmt.Errorf(
			"context injection schema version %d is newer than supported version %d",
			state.SchemaVersion,
			contextInjectionStoreSchemaVersion,
		)
	}
	if state.Injections == nil {
		state.Injections = make(map[string]contextinjectionmodel.Record)
	}
	memory := &memoryContextInjectionStore{
		injections:   make(map[string]contextinjectionmodel.Record, len(state.Injections)),
		nextSequence: state.NextSequence,
	}
	for id, record := range state.Injections {
		if id != record.Injection.ID {
			return nil, fmt.Errorf(
				"context injection map key %q contains ID %q",
				id,
				record.Injection.ID,
			)
		}
		if err := validateLoadedContextRecord(record); err != nil {
			return nil, fmt.Errorf(
				"validate context injection %q: %w",
				id,
				err,
			)
		}
		memory.injections[id] = contextinjectionmodel.Record{
			Sequence:  record.Sequence,
			Injection: sdk.CloneContextInjection(record.Injection),
		}
	}
	return memory, nil
}

func (store *fileContextInjectionStore) writeLocked(
	ctx context.Context,
	memory *memoryContextInjectionStore,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	memory.mu.Lock()
	state := fileContextInjectionState{
		SchemaVersion: contextInjectionStoreSchemaVersion,
		NextSequence:  memory.nextSequence,
		Injections: make(
			map[string]contextinjectionmodel.Record,
			len(memory.injections),
		),
	}
	for id, record := range memory.injections {
		state.Injections[id] = contextinjectionmodel.Record{
			Sequence:  record.Sequence,
			Injection: sdk.CloneContextInjection(record.Injection),
		}
	}
	memory.mu.Unlock()
	return filestate.WriteJSON(
		ctx,
		store.directory,
		store.path,
		"context injections",
		state,
	)
}
