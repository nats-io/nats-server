package metric

import (
	"encoding/json"
	"sync/atomic"
)

type Counter struct {
	value atomic.Uint64
}

func NewCounter() Counter {
	return Counter{}
}

func (c *Counter) Reset() {
	c.value.Store(0)
}

func (c *Counter) Add(delta uint64) {
	c.value.Add(delta)
}

func (c *Counter) Increment() {
	c.Add(1)
}

func (c *Counter) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Count uint64 `json:"COUNT"`
	}{
		Count: c.value.Load(),
	})
}
