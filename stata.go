package stata

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrNotImplemented means feature is not implemented yet
	ErrNotImplemented = errors.New("not implemented")
)

// Stata Stata realtime stat tool
type Stata struct {
	mu      sync.Mutex
	storage *Storage
	cache   *Storage
	events  map[string]*Event
}

// Mode mode manages storage behavior while writing keys
type Mode struct {
	NeedWriteKey func(key KeyValue) bool
}

// ModeDefault this mode writes every update to storage, bypassing cache
var ModeDefault Mode = Mode{
	NeedWriteKey: func(kv KeyValue) bool {
		// write every key update immediately to storage
		return true
	},
}

// ModeReduceWorkload this mode reduces workload to storage by using in-memory cache layer
var ModeReduceWorkload Mode = Mode{
	NeedWriteKey: func(kv KeyValue) bool {
		if kv.Value%10 == 0 {
			return true
		}
		// otherwise write to cache
		return false
	},
}

// Config config
type Config struct {
	Storage *Storage
}

// Storage storage interface for stata
type Storage struct {
	Get      func(key Key) (Value, error)
	Set      func(key Key, val Value) error
	GetRange func(keyRange KeyRange) ([]KeyValue, error)
	IncrBy   func(keys []Key, value Value) error
	Clear    func() error // removes data from storage
}

// Bin keys
type Bin struct {
	Name   string                      // bin name e.g 1m, h, d, month, y
	Format func(t time.Time) time.Time // rounds time to particular time-series bin
}

type bins struct {
	Total  Bin
	Month  Bin
	Year   Bin
	Hour   Bin
	Minute Bin
	Day    Bin
}

// Bins list of default bins
var Bins bins = bins{
	Total: Bin{Name: "total", Format: func(t time.Time) time.Time {
		return time.Date(0, 0, 0, 0, 0, 0, 0, t.Location())
	}},
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

// Value is counter value
type Value = int64

// KeyRange for queries
type KeyRange struct {
	From Key
	To   Key
}

// EventConfig config params for event
type EventConfig struct {
	Bins []Bin
	Mode *Mode
}

// Event creates new event
func (s *Stata) Event(name string, config EventConfig) *Event {
	var mode *Mode = func() *Mode {
		if config.Mode != nil {
			return config.Mode
		}
		return &ModeDefault
	}()
	event := &Event{
		stata: s,
		bins:  config.Bins,
		mode:  mode,
		Name:  name,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events[name] = event
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
func (s *Stata) Get(key Key) (Value, error) {
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
	Name  string
	stata *Stata
	bins  []Bin
	mode  *Mode
}

// KeyValue key-value pair
type KeyValue struct {
	Key   Key
	Value Value
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

	cacheKey := Key{
		Timestamp: time.Now(),
		Name:      e.Name,
		Bin:       Bins.Total,
	}

	val, err := e.stata.cache.Get(cacheKey)
	if err != nil {
		return err
	}
	needWrite := e.mode.NeedWriteKey(KeyValue{
		Key:   cacheKey,
		Value: val,
	})
	// it's time to write keys to storage
	if needWrite {
		err := e.stata.storage.IncrBy(keys, val)
		if err != nil {
			return err
		}
		// reset cache
		err = e.stata.cache.Set(cacheKey, 0)
		if err != nil {
			return err
		}
	} else {
		// inc only in temp
		e.stata.cache.IncrBy([]Key{cacheKey}, 1)
	}

	return nil
}

// New creates new stata client
func New(config *Config) *Stata {
	var storage *Storage = config.Storage
	// use in-memory storage if not initialized
	if storage == nil {
		storage = NewMemoryStorage()
	}
	return &Stata{
		storage: storage,
		events:  make(map[string]*Event),
		cache:   NewMemoryStorage(),
	}
}
