// +build !nacl,!plan9,!linux,!darwin

package syslog

import (
	"errors"
	"io"

	"github.com/kthomas/logrus"
)

// SyslogHook to send logs via syslog.
type SyslogHook struct {
	Writer        io.Writer
	SyslogNetwork string
	SyslogRaddr   string
}

// Creates a hook to be added to an instance of logger. This is called with
// `hook, err := NewSyslogHook("udp", "localhost:514", syslog.LOG_DEBUG, "")`
// `if err == nil { log.Hooks.Add(hook) }`
func NewSyslogHook(network, raddr string, priority int, tag string) (*SyslogHook, error) {
	return nil, errors.New("syslog not implented on windows")
}

func (hook *SyslogHook) Fire(entry *logrus.Entry) error {
	return errors.New("syslog not implented on windows")

}

func (hook *SyslogHook) Levels() []logrus.Level {
	return logrus.AllLevels
}
