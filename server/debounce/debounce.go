package debounce

import (
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Debouncer will debounce fsnotify events
type Debouncer struct {
	duration time.Duration
	mu       sync.Mutex
	timers   map[string]*time.Timer
}

func NewDebouncer(duration time.Duration) *Debouncer {
	return &Debouncer{
		duration: duration,
		timers:   make(map[string]*time.Timer),
	}
}

func (d *Debouncer) Debounce(event fsnotify.Event, callback func(fsnotify.Event)) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if timer, exists := d.timers[event.Name]; exists {
		timer.Stop()
	}

	d.timers[event.Name] = time.AfterFunc(d.duration, func() {
		d.mu.Lock()
		delete(d.timers, event.Name)
		d.mu.Unlock()
		callback(event)
	})
}
