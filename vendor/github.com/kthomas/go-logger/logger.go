package logger

import (
	"fmt"
	"os"
	"strings"

	log "github.com/kthomas/logrus"
	sysloghook "github.com/kthomas/logrus/hooks/syslog"
)

const syslogInfoPriority = 6

// Logger instance
type Logger struct {
	lvl            string
	logger         *log.Logger
	path           *string
	prefix         string
	syslogEndpoint *string
}

// NewLogger initialize logger instance
func NewLogger(prefix string, lvl string, endpoint *string) *Logger {
	lg := Logger{}
	lg.lvl = lvl
	lg.prefix = fmt.Sprintf("%s ", strings.Trim(prefix, " "))
	lg.syslogEndpoint = endpoint

	lg.configure()

	return &lg
}

func (lg *Logger) configure() {
	logger := log.New() //glogger.Init(lg.prefix, lg.console, lg.syslog, lf)
	logger.Formatter = defaultLogFormatter

	logLevel, err := log.ParseLevel(lg.lvl)
	if err != nil {
		logger.Warningf("failed to parse log level %s; %s", lg.lvl, err.Error())
	} else {
		logger.Level = logLevel
	}

	if lg.path != nil {
		logfile, err := os.OpenFile(*lg.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
		if err != nil {
			logger.Warningf("failed to open log file destination path: %s; %s", *lg.path, err.Error())
		}
		defer logfile.Close()

		logger.Out = logfile
	} else if lg.syslogEndpoint != nil {
		hook, err := sysloghook.NewSyslogHook("udp", *lg.syslogEndpoint, syslogInfoPriority, lg.prefix)
		if err != nil {
			log.Errorf("unable to dial syslog daemon; %s", err.Error())
		} else {
			logger.AddHook(hook)
		}
	} else {
		logger.Tracef("using stderr for new logger instance")
		logger.Out = os.Stderr
	}

	lg.logger = logger
}

// Clone a logger instance
func (lg *Logger) Clone() *Logger {
	return &Logger{
		logger:         lg.logger,
		prefix:         lg.prefix,
		path:           lg.path,
		syslogEndpoint: lg.syslogEndpoint,
	}
}

// Critical log
func (lg *Logger) Critical(msg string) {
	lg.logger.Fatal(msg)
}

// Criticalf log
func (lg *Logger) Criticalf(msg string, v ...interface{}) {
	lg.logger.Fatalf(msg, v...)
}

// Debug log
func (lg *Logger) Debug(msg string) {
	lg.logger.Debug(msg)
}

// Debugf log
func (lg *Logger) Debugf(msg string, v ...interface{}) {
	lg.logger.Debugf(msg, v...)
}

// Error log
func (lg *Logger) Error(msg string) {
	lg.logger.Error(msg)
}

// Errorf log
func (lg *Logger) Errorf(msg string, v ...interface{}) {
	lg.logger.Errorf(msg, v...)
}

// Info log
func (lg *Logger) Info(msg string) {
	lg.logger.Info(msg)
}

// Infof log
func (lg *Logger) Infof(msg string, v ...interface{}) {
	lg.logger.Infof(msg, v...)
}

// LogOnError logs an error if the given err is non-nil
func (lg *Logger) LogOnError(err error, s string) bool {
	hasErr := false
	if err != nil {
		msg := fmt.Sprintf("Error: %s", err)
		if s != "" {
			msg = fmt.Sprintf("%s; %s", msg, s)
		}
		lg.Errorf(msg)
		hasErr = true
	}
	return hasErr
}

// Panicf log
func (lg *Logger) Panicf(msg string, v ...interface{}) {
	lg.logger.Fatalf(msg, v...)
}

// PanicOnError panics if the given err is non-nil
func (lg *Logger) PanicOnError(err error, s string) {
	if err != nil {
		msg := fmt.Sprintf("Error: %s", err)
		if s != "" {
			msg = fmt.Sprintf("%s; %s", msg, s)
		}
		lg.Panicf(msg)
	}
}

// Trace log
func (lg *Logger) Trace(msg string) {
	lg.logger.Trace(msg)
}

// Tracef log
func (lg *Logger) Tracef(msg string, v ...interface{}) {
	lg.logger.Tracef(msg, v...)
}

// Warning log
func (lg *Logger) Warning(msg string) {
	lg.logger.Warning(msg)
}

// Warningf log
func (lg *Logger) Warningf(msg string, v ...interface{}) {
	lg.logger.Warningf(msg, v...)
}
