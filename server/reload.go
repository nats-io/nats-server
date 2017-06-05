package server

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// FlagSnapshot captures the server options as specified by CLI flags at
// startup. This should not be modified once the server has started.
var FlagSnapshot = &Options{}

// option is a hot-swappable configuration setting.
type option interface {
	// Apply the server option.
	Apply(server *Server)
}

// traceOption implements the option interface for the `trace` setting.
type traceOption struct {
	newValue bool
}

// Apply the tracing change by reconfiguring the server's logger.
func (t *traceOption) Apply(server *Server) {
	server.ConfigureLogger()
	server.Noticef("Reloaded: trace = %v", t.newValue)
}

// Reload reads the current configuration file and applies any supported
// changes. This returns an error if the server was not started with a config
// file or an option which doesn't support hot-swapping was changed.
func (s *Server) Reload() error {
	if s.configFile == "" {
		return errors.New("Can only reload config when a file is provided using -c or --config")
	}
	newOpts, err := ProcessConfigFile(s.configFile)
	if err != nil {
		// TODO: Dump previous good config to a .bak file?
		return fmt.Errorf("Config reload failed: %s", err)
	}
	// Apply flags over config file settings.
	newOpts = MergeOptions(newOpts, FlagSnapshot)
	processOptions(newOpts)
	return s.reloadOptions(newOpts)
}

// reloadOptions reloads the server config with the provided options. If an
// option that doesn't support hot-swapping is changed, this returns an error.
func (s *Server) reloadOptions(newOpts *Options) error {
	changed, err := s.diffOptions(newOpts)
	if err != nil {
		return err
	}
	s.setOpts(newOpts)
	s.applyOptions(changed)
	return nil
}

// diffOptions returns a slice containing options which have been changed. If
// an option that doesn't support hot-swapping is changed, this returns an
// error.
func (s *Server) diffOptions(newOpts *Options) ([]option, error) {
	var (
		oldConfig = reflect.ValueOf(s.opts).Elem()
		newConfig = reflect.ValueOf(newOpts).Elem()
		diffOpts  = []option{}
	)

	for i := 0; i < oldConfig.NumField(); i++ {
		var (
			field    = oldConfig.Type().Field(i)
			oldValue = oldConfig.Field(i).Interface()
			newValue = newConfig.Field(i).Interface()
			changed  = !reflect.DeepEqual(oldValue, newValue)
		)
		if !changed {
			continue
		}
		switch strings.ToLower(field.Name) {
		case "trace":
			diffOpts = append(diffOpts, &traceOption{newValue.(bool)})
		default:
			// Bail out if attempting to reload any unsupported options.
			return nil, fmt.Errorf("Config reload not supported for %s", field.Name)
		}
	}

	return diffOpts, nil
}

func (s *Server) applyOptions(opts []option) {
	for _, opt := range opts {
		opt.Apply(s)
	}

	s.Noticef("Reloaded server configuration")
}
