package metric

import (
	"encoding/json"
	"strings"
	"sync"
)

// Metric is the interface each metric implements.
type Metric interface {
	Reset()
	MarshalJSON() ([]byte, error)
}

// Registry stores metrics in a hierarchical path tree.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*Registry
	metric  Metric
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		entries: make(map[string]*Registry),
	}
}

// Add registers a metric at the given path
func (r *Registry) Add(path string, m Metric) {
	r.mu.Lock()
	defer r.mu.Unlock()

	parts := strings.Split(strings.Trim(path, "."), ".")
	curr := r
	for _, p := range parts {
		if curr.entries[p] == nil {
			curr.entries[p] = NewRegistry()
		}
		curr = curr.entries[p]
	}
	curr.metric = m
}

// Query returns all metrics under the given path as JSON.
func (r *Registry) Query(path string) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if path == "" || path == "/" {
		// Root query — marshal entire registry
		data, err := json.MarshalIndent(r, "", "  ")
		return data, err
	}

	node := r.getNode(path)
	if node == nil {
		// Return empty JSON
		data, _ := json.Marshal(map[string]any{})
		return data, nil
	}

	data, _ := json.MarshalIndent(node.toMap(), "", "  ")
	return data, nil
}

func (r *Registry) Reset(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	node := r.getNode(path)
	if node == nil {
		return // nothing to reset
	}

	node.resetAll()
}

func (r *Registry) MarshalJSON() ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	m := r.toMap()
	return json.Marshal(m)
}

// getNode navigates to the node corresponding to the given path.
func (r *Registry) getNode(path string) *Registry {
	parts := strings.Split(strings.Trim(path, "."), ".")
	curr := r
	for _, p := range parts {
		next, ok := curr.entries[p]
		if !ok {
			return nil
		}
		curr = next
	}
	return curr
}

// toMap recursively converts the tree to a nested map[string]interface{}.
func (r *Registry) toMap() map[string]any {
	result := make(map[string]any)

	// Inline metric payload at this node.
	if r.metric != nil {
		var m any
		bytes, _ := r.metric.MarshalJSON()
		_ = json.Unmarshal(bytes, &m)
		if mm, ok := m.(map[string]any); ok {
			//			maps.Copy(result, mm)
			for k, v := range mm {
				result[k] = v
			}
		} else {
			result["value"] = m
		}
	}

	// include sub-entries
	for name, sub := range r.entries {
		result[name] = sub.toMap()
	}

	return result
}

func (r *Registry) resetAll() {
	if r.metric != nil {
		r.metric.Reset()
	}
	for _, sub := range r.entries {
		sub.resetAll()
	}
}
