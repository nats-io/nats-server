package metric

import "encoding/json"

type Histogram struct {
	cnt int
	min int
	max int
	bin []int
}

func NewHistogram(max int) Histogram {
	return Histogram{bin: make([]int, max)}
}

func (h *Histogram) Push(sample int) {
	if h.cnt == 0 {
		h.min = sample
		h.max = sample
	} else {
		h.min = min(sample, h.min)
		h.max = max(sample, h.max)
	}
	h.cnt++
	if sample >= len(h.bin) {
		sample = len(h.bin) - 1
	}
	h.bin[sample]++
}

func (h *Histogram) Count() int {
	return h.cnt
}

func (h *Histogram) Min() int {
	return h.min
}

func (h *Histogram) Max() int {
	return h.max
}

func (h *Histogram) Reset() {
	clear(h.bin)
	h.cnt, h.min, h.max = 0, 0, 0
}

func (h *Histogram) ValueAtPercentile(p int) int {
	if h.cnt == 0 {
		return 0
	}
	if p <= 0 {
		return h.min
	}
	if p >= 100 {
		return h.max
	}

	target := (h.cnt * p) / 100
	if target == 0 {
		return h.min
	}

	sum := 0
	for i, c := range h.bin {
		sum += c
		if sum >= target {
			return i
		}
	}
	return h.max
}

func (h *Histogram) compactBins() map[int]int {
	m := make(map[int]int)
	for i, c := range h.bin {
		if c > 0 {
			m[i] = c
		}
	}
	return m
}

func (h *Histogram) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Min   int         `json:"MIN"`
		Max   int         `json:"MAX"`
		Count int         `json:"COUNT"`
		Bins  map[int]int `json:"BINS"`
	}{
		Min:   h.min,
		Max:   h.max,
		Count: h.cnt,
		Bins:  h.compactBins(),
	})
}
