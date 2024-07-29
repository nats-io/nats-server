package nhist

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewHistogramRecorder(t *testing.T) {
	interval := time.Minute
	mark := time.Now().UTC()
	source := "test_source"
	subject := "test_subject"

	hr := NewHistogramRecorder(interval, mark, source, subject)

	if hr == nil {
		t.Fatal("NewHistogramRecorder returned nil")
	}

	if hr.interval != interval {
		t.Errorf("Expected interval %v, got %v", interval, hr.interval)
	}

	if hr.mark != mark {
		t.Errorf("Expected mark %v, got %v", mark, hr.mark)
	}

	if hr.source != source {
		t.Errorf("Expected source %s, got %s", source, hr.source)
	}

	if hr.subject != subject {
		t.Errorf("Expected subject %s, got %s", subject, hr.subject)
	}
}

func TestRecordDuration(t *testing.T) {
	hr := NewHistogramRecorder(time.Minute, time.Now().UTC(), "test", "test")

	duration := time.Second
	err := hr.RecordDuration(duration)

	if err != nil {
		t.Errorf("RecordDuration returned unexpected error: %v", err)
	}

	if count := hr.GetCount(); count != 1 {
		t.Errorf("Expected count 1, got %d", count)
	}
}

func TestGetInterval(t *testing.T) {
	interval := time.Hour
	hr := NewHistogramRecorder(interval, time.Now().UTC(), "test", "test")

	if got := hr.GetInterval(); got != interval {
		t.Errorf("GetInterval() = %v, want %v", got, interval)
	}
}

func TestGetCount(t *testing.T) {
	hr := NewHistogramRecorder(time.Minute, time.Now().UTC(), "test", "test")

	if count := hr.GetCount(); count != 0 {
		t.Errorf("Initial count should be 0, got %d", count)
	}

	hr.RecordDuration(time.Second)

	if count := hr.GetCount(); count != 1 {
		t.Errorf("Count after recording should be 1, got %d", count)
	}
}

func TestMarshal(t *testing.T) {
	hr := NewHistogramRecorder(time.Minute, time.Now().UTC(), "test_source", "test_subject")
	hr.RecordDuration(time.Second)

	data, err := hr.Marshal()
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	var msg HistogramRecorderMsg
	err = json.Unmarshal(data, &msg)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if msg.Source != "test_source" {
		t.Errorf("Expected source 'test_source', got '%s'", msg.Source)
	}

	if msg.Subject != "test_subject" {
		t.Errorf("Expected subject 'test_subject', got '%s'", msg.Subject)
	}

	if msg.Interval != time.Minute {
		t.Errorf("Expected interval %v, got %v", time.Minute, msg.Interval)
	}

	if msg.Histogram.Count() != 1 {
		t.Errorf("Expected histogram count 1, got %d", msg.Histogram.Count())
	}
}

func TestUnmarshal(t *testing.T) {
	hr := NewHistogramRecorder(time.Minute, time.Now().UTC(), "test_source", "test_subject")
	hr.RecordDuration(time.Second)

	data, _ := hr.Marshal()

	msg, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if msg.Source != "test_source" {
		t.Errorf("Expected source 'test_source', got '%s'", msg.Source)
	}

	if msg.Subject != "test_subject" {
		t.Errorf("Expected subject 'test_subject', got '%s'", msg.Subject)
	}

	if msg.Interval != time.Minute {
		t.Errorf("Expected interval %v, got %v", time.Minute, msg.Interval)
	}

	if msg.Histogram.Count() != 1 {
		t.Errorf("Expected histogram count 1, got %d", msg.Histogram.Count())
	}
}

func TestResetWithMark(t *testing.T) {
	hr := NewHistogramRecorder(time.Minute, time.Now().UTC(), "test", "test")
	hr.RecordDuration(time.Second)

	newMark := time.Now().UTC().Add(time.Hour)
	hr.ResetWithMark(newMark)

	if hr.GetCount() != 0 {
		t.Errorf("Expected count 0 after reset, got %d", hr.GetCount())
	}

	if hr.mark != newMark {
		t.Errorf("Expected mark %v, got %v", newMark, hr.mark)
	}
}

func TestTwoRoundsMarshalCount(t *testing.T) {
	hr := NewHistogramRecorder(time.Second, time.Now().UTC(), "test_source", "test_subject")

	// First round: 1000 values
	for i := 0; i < 1000; i++ {
		err := hr.RecordDuration(time.Duration(i) * time.Millisecond)
		if err != nil {
			t.Fatalf("Failed to record duration in first round: %v", err)
		}
	}

	// Marshal after first round
	data1, err := hr.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal after first round: %v", err)
	}

	msg1 := &HistogramRecorderMsg{}
	err = json.Unmarshal(data1, msg1)
	if err != nil {
		t.Fatalf("Failed to unmarshal after first round: %v", err)
	}

	if count := msg1.Histogram.Count(); count != 1000 {
		t.Errorf("Expected count 1000 after first round, got %d", count)
	}

	// Second round: 999 values
	for i := 0; i < 999; i++ {
		err := hr.RecordDuration(time.Duration(i) * time.Millisecond)
		if err != nil {
			t.Fatalf("Failed to record duration in second round: %v", err)
		}
	}

	// Marshal after second round
	data2, err := hr.Marshal()
	if err != nil {
		t.Fatalf("Failed to marshal after second round: %v", err)
	}

	msg2 := &HistogramRecorderMsg{}
	err = json.Unmarshal(data2, msg2)
	if err != nil {
		t.Fatalf("Failed to unmarshal after second round: %v", err)
	}

	if count := msg2.Histogram.Count(); count != 999 {
		t.Errorf("Expected count 999 after second round, got %d", count)
	}

	// Check that the original HistogramRecorder was reset after marshaling
	if count := hr.GetCount(); count != 0 {
		t.Errorf("Expected HistogramRecorder count to be 0 after marshaling, got %d", count)
	}
}
