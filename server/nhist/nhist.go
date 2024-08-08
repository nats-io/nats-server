/*
 * These histograms are supposed to behave like prometheus native
 * histograms with 1 <= schema <= 8.  See prometheus/histogram.go in
 *
 * https://github.com/prometheus/client_golang/prometheus
 *
 * Considerable code and logic is gratefully cribbed therefrom.
 * But the API is based on Circonus' log-linear histograms, see
 *
 * https://github.com/openhistogram/circonusllhist
 */

package nhist

import (
	"cmp"
	"encoding/json"
	"errors"
	//"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Bounds []float64

type Tranch [256]int64 // atomic, but we must be able to copy them

type Histogram struct {
	lock       sync.RWMutex // need write lock to copy or reset
	bounds     Bounds
	zeroThresh float64 // width/2 of the bin around zero
	sum        Float64 // an atomic one
	count      atomic.Int64
	zeroCount  atomic.Int64
	tranches   []*Tranch // pos. value bins followed by neg.
}

var HistogramBounds [9]Bounds

func init() {
	var prev Bounds
	for i := range HistogramBounds {
		factor := math.Exp2(math.Exp2(float64(-i)))
		bounds := make(Bounds, 1<<i)
		bounds[0] = 0.5
		for j := 1; j < len(bounds); j++ {
			if j%2 == 0 {
				bounds[j] = prev[j/2]
			} else {
				bounds[j] = bounds[j-1] * factor
			}
		}
		HistogramBounds[i] = bounds
		prev = bounds
	}
}

type Option func(*Histogram)

func WithSchema(k int) Option {
	if k <= 0 || k > 8 { // don't panic,
		k = 4 // just use default
	}
	return func(h *Histogram) {
		h.bounds = HistogramBounds[k]
	}
}

func WithZeroThresh(t float64) Option {
	return func(h *Histogram) {
		h.zeroThresh = t
	}
}

/*
 * how many Tranches do we need (see below)?
 *
 * GetBin returns
 *
 *     key + (exp+125)*len(bounds)
 *
 * where 0 <= key < len(bounds) and -125 <= exp <= 128.
 * Hence the smallest bin number is 0 and the largest is
 *
 *     len(bounds)-1 + (128+125)*len(bounds)
 *
 *     254*len(bounds) - 1
 *
 * a Tranch holds 256 bins, so len(bounds) Tranches
 * will suffice.  we allocate (in a single slice)
 * two sets of Tranch pointers, one for positive values
 * and one for negative.
 */
func New(opts ...Option) *Histogram {
	h := new(Histogram)
	for _, opt := range opts {
		opt(h)
	}
	if h.bounds == nil {
		h.bounds = HistogramBounds[4]
	}
	if h.zeroThresh == 0.0 {
		h.zeroThresh = h.UpperEdge(0)
	}
	h.tranches = make([]*Tranch, 2*len(h.bounds)) // see above
	return h
}

const (
	NoReset = iota
	Reset
	FullReset
)

func (h *Histogram) Copy(optReset ...int) *Histogram {
	h.lock.Lock()
	defer h.lock.Unlock()
	hc := Histogram{
		bounds:     h.bounds,
		zeroThresh: h.zeroThresh,
		tranches:   make([]*Tranch, len(h.tranches)),
	}
	hc.sum.Store(h.sum.Load())
	hc.count.Store(h.count.Load())
	hc.zeroCount.Store(h.zeroCount.Load())
	reset := cmp.Or(optReset...)
	if reset != NoReset {
		h.sum.Store(0)
		h.count.Store(0)
		h.zeroCount.Store(0)
	}
	for tidx, tr := range h.tranches {
		if tr != nil {
			if reset == FullReset {
				hc.tranches[tidx] = tr
				h.tranches[tidx] = nil
				continue
			}
			ctr := new(Tranch)
			*ctr = *tr
			hc.tranches[tidx] = ctr
			if reset != NoReset {
				clear(tr[:])
			}
		}
	}
	return &hc
}

func (h *Histogram) Reset(optFullReset ...int) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.sum.Store(0)
	h.count.Store(0)
	h.zeroCount.Store(0)
	if cmp.Or(optFullReset...) == FullReset {
		clear(h.tranches)
		return
	}
	for _, tr := range h.tranches {
		if tr != nil {
			clear(tr[:])
		}
	}
}

func (bounds Bounds) GetBin(v float64) int {
	if v == 0 {
		return 0
	}
	v = math.Abs(v)
	if math.IsNaN(v) || v > math.MaxFloat32 {
		return -1
	}
	frac, exp := math.Frexp(v)
	if exp < -125 { // smallest normalized float32
		return -1
	}
	// bins are (lo, hi] in prometheus histograms
	key := sort.SearchFloat64s(bounds, frac)
	if key == len(bounds) {
		key = 0
		exp++
	}
	return key + (exp+125)*len(bounds)
}

func (h *Histogram) UpperEdge(bin int) float64 {
	if bin < 0 {
		return math.NaN()
	}
	k, e := bin%len(h.bounds), bin/len(h.bounds)
	if e > 128 {
		return math.NaN()
	}
	return math.Ldexp(h.bounds[k], e-125)
}

func (h *Histogram) LowerEdge(bin int) float64 {
	if bin > 0 {
		return h.UpperEdge(bin - 1)
	}
	return h.UpperEdge(len(h.bounds)-1) / 2.0
}

func (h *Histogram) Width(bin int) float64 {
	return h.UpperEdge(bin) - h.LowerEdge(bin)
}

func (h *Histogram) Center(bin int) float64 {
	return (h.UpperEdge(bin) + h.LowerEdge(bin)) / 2.0
}

var ErrOutOfRange = errors.New("value out of range")

func (h *Histogram) RecordValues(v float64, n int) error {
	h.lock.RLock()
	defer h.lock.RUnlock()
	if math.Abs(v) <= h.zeroThresh {
		h.zeroCount.Add(1)
	} else {
		bin := h.bounds.GetBin(v)
		if bin < 0 {
			return ErrOutOfRange
		}
		tidx := bin / 256
		if v < 0.0 {
			tidx += len(h.bounds)
		}
		tr := h.tranches[tidx]
		if tr == nil {
			h.lock.RUnlock()
			h.lock.Lock()
			tr = h.tranches[tidx]
			if tr == nil {
				tr = new(Tranch)
				h.tranches[tidx] = tr
			}
			h.lock.Unlock()
			h.lock.RLock()
		}
		atomic.AddInt64(&tr[bin%256], int64(n))
	}
	h.count.Add(int64(n))
	h.sum.Add(v * float64(n))
	return nil
}

func (h *Histogram) RecordValue(v float64) error {
	return h.RecordValues(v, 1)
}

func (h *Histogram) RecordDuration(d time.Duration) error {
	return h.RecordValues(d.Seconds(), 1)
}

func (h *Histogram) Mean() float64 {
	h.lock.Lock()
	defer h.lock.Unlock()
	n := h.count.Load()
	if n == 0 {
		return math.NaN()
	}
	return h.sum.Load() / float64(n)
}

func (h *Histogram) Count() int64 {
	return h.count.Load()
}

type serializableHistogram struct {
	Schema     int
	ZeroThresh float64
	Sum        float64
	Count      int64
	ZeroCount  int64
	Tranches   []*Tranch
}

func (h *Histogram) MarshalJSON() ([]byte, error) {
	h.lock.Lock()
	defer h.lock.Unlock()
	sh := serializableHistogram{
		Schema:     math.Ilogb(float64(len(h.bounds))),
		ZeroThresh: h.zeroThresh,
		Sum:        h.sum.Load(),
		Count:      h.count.Load(),
		ZeroCount:  h.zeroCount.Load(),
		Tranches:   h.tranches,
	}
	return json.Marshal(&sh)
}

func (h *Histogram) UnmarshalJSON(buf []byte) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	sh := new(serializableHistogram)
	if err := json.Unmarshal(buf, sh); err != nil {
		return err
	}
	h.bounds = HistogramBounds[sh.Schema]
	h.zeroThresh = sh.ZeroThresh
	h.sum.Store(sh.Sum)
	h.count.Store(sh.Count)
	h.zeroCount.Store(sh.ZeroCount)
	h.tranches = sh.Tranches
	return nil
}

func UnmarshalJSON(buf []byte) (*Histogram, error) {
	h := new(Histogram)
	if err := h.UnmarshalJSON(buf); err != nil {
		return nil, err
	}
	return h, nil
}
