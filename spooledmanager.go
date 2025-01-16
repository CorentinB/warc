package warc

import (
	"container/heap"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// SpoolManager enforces a global memory limit, tracks spoolers, and handles eviction.
type SpoolManager struct {
	mu                sync.Mutex
	spoolers          spoolHeap
	spoolerIndex      map[*spooledTempFile]*spoolItem
	currentMemUsage   int64
	GlobalMemoryLimit int64
}

type spoolItem struct {
	s        *spooledTempFile
	priority time.Time // used to determine which spooler is oldest (min-heap)
	index    int       // heap interface requirement
}

type spoolHeap []*spoolItem

func (h spoolHeap) Len() int { return len(h) }

func (h spoolHeap) Less(i, j int) bool {
	return h[i].priority.Before(h[j].priority)
}

func (h spoolHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *spoolHeap) Push(x interface{}) {
	item := x.(*spoolItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *spoolHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// DefaultSpoolManager is the global manager. Adjust limit as desired.
var DefaultSpoolManager = NewSpoolManager(getHalfOfAvailableRAM())

func NewSpoolManager(limit int64) *SpoolManager {
	m := &SpoolManager{
		GlobalMemoryLimit: limit,
		spoolerIndex:      make(map[*spooledTempFile]*spoolItem),
	}
	heap.Init(&m.spoolers)
	return m
}

func (m *SpoolManager) RegisterSpool(s *spooledTempFile) {
	m.mu.Lock()
	defer m.mu.Unlock()
	item := &spoolItem{
		s:        s,
		priority: time.Now(),
	}
	m.spoolerIndex[s] = item
	heap.Push(&m.spoolers, item)
}

func (m *SpoolManager) UnregisterSpool(s *spooledTempFile) {
	m.mu.Lock()
	defer m.mu.Unlock()
	item, ok := m.spoolerIndex[s]
	if !ok {
		return
	}
	delete(m.spoolerIndex, s)
	heap.Remove(&m.spoolers, item.index)
}

func (m *SpoolManager) CanAddBytes(n int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.currentMemUsage+int64(n) <= m.GlobalMemoryLimit
}

func (m *SpoolManager) AddBytes(n int) {
	m.mu.Lock()
	m.currentMemUsage += int64(n)
	m.mu.Unlock()
}

func (m *SpoolManager) SubBytes(n int) {
	m.mu.Lock()
	m.currentMemUsage -= int64(n)
	if m.currentMemUsage < 0 {
		m.currentMemUsage = 0
	}
	m.mu.Unlock()
}

func (m *SpoolManager) EvictIfNeeded() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for m.currentMemUsage > m.GlobalMemoryLimit && len(m.spoolers) > 0 {
		item := m.spoolers[0]
		if item.s.file == nil && !item.s.closed {
			item.s.forceToDiskIfInMemory()
		} else {
			// If it's already on disk or closed, pop it to avoid looping
			heap.Remove(&m.spoolers, item.index)
			delete(m.spoolerIndex, item.s)
		}
	}
}

func getHalfOfAvailableRAM() int64 {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		panic(err)
	}
	return int64(info.Totalram) / 2
}
