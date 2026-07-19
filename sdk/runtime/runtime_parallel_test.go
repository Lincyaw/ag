package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunParallelIndexedHonorsLimit(t *testing.T) {
	t.Parallel()
	var active int64
	var peak int64
	errs := runParallelIndexed(
		context.Background(),
		8,
		parallelIndexedOptions{Limit: 2},
		func(context.Context, int) error {
			current := atomic.AddInt64(&active, 1)
			for {
				previous := atomic.LoadInt64(&peak)
				if current <= previous ||
					atomic.CompareAndSwapInt64(&peak, previous, current) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt64(&active, -1)
			return nil
		},
	)
	for index, err := range errs {
		if err != nil {
			t.Fatalf("task %d error = %v", index, err)
		}
	}
	if peak > 2 {
		t.Fatalf("peak concurrency = %d", peak)
	}
}
