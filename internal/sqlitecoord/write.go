package sqlitecoord

import (
	"hash/fnv"
	"sync"
)

// SQLite permits one writer per database file. A gateway opens separate GORM
// stores for its control projection and trajectory namespaces, so coordinate
// their write transactions before SQLite has to resolve a read-to-write
// upgrade race. Cross-process contention is still handled by WAL and the
// database busy timeout.
const writeMutexStripes = 64

var writeMutexes [writeMutexStripes]sync.Mutex
var openMutex sync.Mutex

// OpenMutex serializes the modernc SQLite driver's process-global
// initialization. Once connections are open, per-file coordination below is
// sufficient.
func OpenMutex() *sync.Mutex { return &openMutex }

func WriteMutex(path string) *sync.Mutex {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(path))
	return &writeMutexes[hash.Sum32()%writeMutexStripes]
}
