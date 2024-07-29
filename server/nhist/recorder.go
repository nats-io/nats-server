package nhist

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/openhistogram/circonusllhist"
)

type HistogramRecorder struct {
	sync.Mutex
	histogram *circonusllhist.Histogram
	mark      time.Time
	interval  time.Duration
	source    string
	subject   string
}

type HistogramRecorderMsg struct {
	Histogram *circonusllhist.Histogram `json:"histogram"`
	Mark      time.Time                 `json:"mark"`
	Interval  time.Duration             `json:"interval"`
	Source    string                    `json:"source"`
	Subject   string                    `json:"subject"`
}

func NewHistogramRecorder(interval time.Duration, mark time.Time, source, subject string) *HistogramRecorder {
	hr := &HistogramRecorder{
		histogram: circonusllhist.New(),
		mark:      mark,
		interval:  interval,
		source:    source,
		subject:   subject,
	}
	return hr
}

func (hr *HistogramRecorder) RecordDuration(v time.Duration) error {
	hr.Lock()
	defer hr.Unlock()
	return hr.histogram.RecordDuration(v)
}

func (hr *HistogramRecorder) GetInterval() time.Duration {
	hr.Lock()
	defer hr.Unlock()
	return hr.interval
}

func (hr *HistogramRecorder) GetSubject() string {
	hr.Lock()
	defer hr.Unlock()
	return hr.subject
}

func (hr *HistogramRecorder) GetCount() uint64 {
	hr.Lock()
	defer hr.Unlock()
	return hr.histogram.Count()
}

func (hr *HistogramRecorder) ResetWithMark(mark time.Time) {
	hr.Lock()
	defer hr.Unlock()
	hr.histogram.FullReset()
	hr.mark = mark
}

func (hr *HistogramRecorder) Marshal() ([]byte, error) {
	hr.Lock()
	msg := &HistogramRecorderMsg{
		Histogram: hr.histogram,
		Mark:      hr.mark,
		Interval:  hr.interval,
		Source:    hr.source,
		Subject:   hr.subject,
	}

	//fmt.Printf("MSG: %+v\n", msg)
	//fmt.Printf("HISTOGRAM: %+v\n", msg.Histogram.DecStrings())

	jmsg, err := json.Marshal(msg)
	if err != nil {
		fmt.Printf("json err %v\n", err)
	}

	// We would want to use Reset() to avoid reallocation of bins,
	// but the current implementation does not do this properly.
	hr.histogram.FullReset()
	hr.mark = time.Now().UTC()
	hr.Unlock()
	return jmsg, err
}

func Unmarshal(msg []byte) (*HistogramRecorderMsg, error) {
	hrm := &HistogramRecorderMsg{}
	err := json.Unmarshal(msg, hrm)
	return hrm, err
}
