package cli

import "sync"

const progressQueueSize = 256

type progressRecordQueue struct {
	records chan progressRecord
	done    chan struct{}
	once    sync.Once
	mu      sync.Mutex
	deliver func(progressRecord)
	started bool
	closed  bool
	dropped uint64
}

func newProgressRecordQueue(deliver func(progressRecord)) *progressRecordQueue {
	return &progressRecordQueue{
		records: make(chan progressRecord, progressQueueSize),
		done:    make(chan struct{}),
		deliver: deliver,
	}
}

func (queue *progressRecordQueue) start() {
	if queue == nil {
		return
	}
	queue.once.Do(func() {
		queue.mu.Lock()
		queue.started = true
		queue.mu.Unlock()
		go func() {
			defer close(queue.done)
			for record := range queue.records {
				if queue.deliver != nil {
					queue.deliver(record)
				}
			}
		}()
	})
}

func (queue *progressRecordQueue) push(record progressRecord) {
	if queue == nil {
		return
	}
	queue.start()
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.closed {
		return
	}
	select {
	case queue.records <- record:
	default:
		queue.dropped++
	}
}

func (queue *progressRecordQueue) close() uint64 {
	if queue == nil {
		return 0
	}
	queue.mu.Lock()
	started := queue.started
	dropped := queue.dropped
	if !queue.closed {
		queue.closed = true
		close(queue.records)
	}
	queue.mu.Unlock()
	if started {
		<-queue.done
	}
	return dropped
}
