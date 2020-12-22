package stata

import (
	"errors"
	"sync"
	"time"
)

// Stata Stata realtime stat tool
type Stata struct {
	mu      sync.Mutex
	storage *Storage
	events  map[string]*Event
}

// Config config
type Config struct {
	Storage *Storage
}

// Storage redis storage for stata
type Storage struct {
	Get      func(keys Key) (int64, error)
	GetRange func(keyRange KeyRange) ([]KeyValue, error)
	IncrBy   func(keys []Key, value int64) error
	Clear    func() error // removes
}

// Bin keys
type Bin struct {
	Name   string                      // bin name e.g 1m, h, d, month, y
	Format func(t time.Time) time.Time // rounds time to particular time-series bin
}

type bins struct {
	Month  Bin
	Year   Bin
	Hour   Bin
	Minute Bin
	Day    Bin
}

// Bins list of default bins
var Bins bins = bins{
	Year: Bin{Name: "y", Format: func(t time.Time) time.Time {
		return time.Date(t.Year(), 0, 0, 0, 0, 0, 0, t.Location())
	}},
	Month: Bin{Name: "m", Format: func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), 0, 0, 0, 0, 0, t.Location())
	}},
	Day: Bin{Name: "d", Format: func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	}},
	Hour: Bin{Name: "h", Format: func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	}},
	Minute: Bin{Name: "1min", Format: func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	}},
}

// Key represents key parts
type Key struct {
	Name      string
	Timestamp time.Time
	Bin       Bin
}

// KeyRange for queries
type KeyRange struct {
	From Key
	To   Key
}

// Event creates new event
func (s *Stata) Event(name string, bins []Bin) *Event {
	event := &Event{
		stata: s,
		bins:  bins,
		Name:  name,
	}
	s.mu.Lock()
	s.events[name] = event
	s.mu.Unlock()
	return event
}

// GetRange gets range of keys
func (s *Stata) GetRange(keyRange KeyRange) ([]KeyValue, error) {
	kv, err := s.storage.GetRange(keyRange)
	if err != nil {
		return nil, err
	}
	return kv, nil
}

// Get increments all counters for that event
func (s *Stata) Get(key Key) (int64, error) {
	if s.storage == nil {
		return 0, errors.New("storage is not initialized yet")
	}
	val, err := s.storage.Get(key)
	if err != nil {
		return 0, err
	}
	return val, nil
}

// GetEvents returns list of events
func (s *Stata) GetEvents() []*Event {
	events := []*Event{}
	for _, event := range s.events {
		events = append(events, event)
	}
	return events
}

// Event foundationdb house event
type Event struct {
	stata *Stata
	bins  []Bin
	Name  string
}

// KeyValue key-value pair
type KeyValue struct {
	Key   Key
	Value int64
}

// Inc increments counters for event
func (e *Event) Inc() error {
	var keys []Key = []Key{}

	for _, bin := range e.bins {
		key := Key{
			Timestamp: time.Now(),
			Name:      e.Name,
			Bin:       bin,
		}
		keys = append(keys, key)
	}

	if e.stata.storage == nil {
		return errors.New("storage is not initialized yet")
	}
	err := e.stata.storage.IncrBy(keys, 1)
	if err != nil {
		return err
	}
	return nil
}

// New creates new stata client
func New(config *Config) *Stata {
	return &Stata{
		storage: config.Storage,
		events:  make(map[string]*Event),
	}
}
