package common

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

var (
	daemon    *usageDaemon
	waitGroup sync.WaitGroup
)

// APICall struct
type APICall struct {
	Sub           string    `json:"sub"`
	Method        string    `json:"method"`
	Host          string    `json:"host"`
	Path          string    `json:"path"`
	RemoteAddr    string    `json:"remote_addr"`
	StatusCode    int       `json:"status_code"`
	ContentLength *uint     `json:"content_length"`
	Timestamp     time.Time `json:"timestamp"`
}

type usageDaemon struct {
	q                   chan *APICall
	bufferSize          int
	delegate            UsageDelegate
	flushIntervalMillis uint
	lastFlushTimestamp  time.Time
	mutex               *sync.Mutex
	sleepIntervalMillis uint

	shutdown context.Context
	cancelF  context.CancelFunc
}

// UsageDelegate interface for API call tracking interface
type UsageDelegate interface {
	Track(*APICall)
}

// RunAPIUsageDaemon initializes and starts a new API usage daemon using the given delegate;
// returns an error if there is already an API usage daemon running as it is currently treated
// as a singleton
func RunAPIUsageDaemon(bufferSize int, flushIntervalMillis uint, delegate UsageDelegate) error {
	if daemon != nil {
		msg := "attempted to run API usage daemon after singleton instance started"
		Log.Warningf(msg)
		return fmt.Errorf(msg)
	}

	daemon = new(usageDaemon)
	daemon.shutdown, daemon.cancelF = context.WithCancel(context.Background())
	daemon.q = make(chan *APICall, bufferSize)
	daemon.bufferSize = bufferSize
	daemon.delegate = delegate
	daemon.flushIntervalMillis = flushIntervalMillis
	daemon.mutex = &sync.Mutex{}
	daemon.lastFlushTimestamp = time.Now()
	go daemon.run()

	return nil
}

func (d *usageDaemon) run() error {
	Log.Debugf("running API usage daemon...")
	ticker := time.NewTicker(time.Duration(d.flushIntervalMillis) * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			if len(d.q) > 0 {
				d.flush()
			}
		case <-d.shutdown.Done():
			Log.Debugf("Flushing API usage daemon on shutdown")
			ticker.Stop()
			return d.flush()
		}
	}
}

func (d *usageDaemon) flush() error {
	for {
		select {
		case apiCall, ok := <-d.q:
			if ok {
				Log.Debugf("attempting to track API call consumed by subject: %s", apiCall.Sub)
				d.delegate.Track(apiCall)
			} else {
				Log.Warningf("failed to receive message from API usage daemon")
			}
		default:
			if len(d.q) == 0 {
				Log.Debugf("API usage daemon buffered channel flushed")
				return nil
			}
		}
	}
}

// newAPICall initializes an API call for tracking API usage
// for a given gin context and parsed JWT subject
func newAPICall(c *gin.Context, sub string) *APICall {
	var contentLength *uint
	contentLengthHeader := c.GetHeader("content-length")
	if contentLengthHeader != "" {
		contentLengthHeaderVal, err := strconv.Atoi(contentLengthHeader)
		if err == nil {
			_contentLength := uint(contentLengthHeaderVal)
			contentLength = &_contentLength
		}
	}

	var remoteAddr string
	xForwardedForHeader := c.GetHeader("x-forwarded-for")
	if xForwardedForHeader != "" {
		remoteAddr = xForwardedForHeader
	} else {
		remoteAddr = c.Request.RemoteAddr
	}

	return &APICall{
		Sub:           sub,
		Method:        c.Request.Method,
		Host:          c.Request.Host,
		Path:          c.Request.URL.Path,
		RemoteAddr:    remoteAddr,
		StatusCode:    c.Writer.Status(),
		ContentLength: contentLength,
		Timestamp:     time.Now(),
	}
}

// TrackAPICall tracks the api call for the given subject and gin context
func TrackAPICall(c *gin.Context, subject string) error {
	if daemon == nil {
		return fmt.Errorf("failed to track API call; singleton usage daemon not initialized")
	}

	if c.GetHeader(authorizationHeader) == "" {
		// no-op
		return fmt.Errorf("failed to track API call; no authorization header provided")
	}

	if subject == "" {
		return fmt.Errorf("failed to resolve subject to which API usage could be attributed")
	}

	Log.Tracef("attempting to track API call for caller: %s", subject)
	daemon.q <- newAPICall(c, subject)
	if len(daemon.q) == daemon.bufferSize {
		go daemon.flush()
	}

	return nil
}
