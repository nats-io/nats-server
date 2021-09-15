//go:build ignore
// +build ignore

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/nats-io/nats-server/v2/server"
)

var tagRe = regexp.MustCompile("\\{(.+?)}")

var templ = `
// Generated code, do not edit. See errors.json and run go generate to update

package server

import "strings"

const (
{{- range $i, $error := . }}
{{- if .Comment }}
	// {{ .Constant }} {{ .Comment }} ({{ .Description | print }})
{{- else }}
	// {{ .Constant }} {{ .Description | print }}
{{- end }}
	{{ .Constant }} ErrorIdentifier = {{ .ErrCode }}  
{{ end }}
)

var (
	ApiErrors = map[ErrorIdentifier]*ApiError{
{{- range $i, $error := . }}
		{{ .Constant }}: {Code: {{ .Code }},ErrCode: {{ .ErrCode }},Description: {{ .Description | printf "%q" }}},{{- end }}
	}

{{- range $i, $error := . }}
{{- if .Deprecates }}
// {{ .Deprecates }} Deprecated by {{ .Constant }} ApiError, use IsNatsError() for comparisons
{{ .Deprecates }} = ApiErrors[{{ .Constant }}]
{{- end }}
{{- end }}
)

{{- range $i, $error := . }}
// {{ .Constant | funcNameForConstant }} creates a new {{ .Constant }} error: {{ .Description | printf "%q" }}
func {{ .Constant | funcNameForConstant }}({{ .Description | funcArgsForTags }}) *ApiError {
    eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
 	}
{{ if .Description | hasTags }}
	e:=ApiErrors[{{.Constant}}]
	args:=e.toReplacerArgs([]interface{}{ {{.Description | replacerArgsForTags }} })
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
{{- else }}
	return ApiErrors[{{.Constant}}]
{{- end }}
}

{{- end }}
`

func panicIfErr(err error) {
	if err == nil {
		return
	}
	panic(err)
}

func goFmt(file string) error {
	c := exec.Command("go", "fmt", file)
	out, err := c.CombinedOutput()
	if err != nil {
		log.Printf("go fmt failed: %s", string(out))
	}

	return err
}

func checkIncrements(errs []server.ErrorsData) error {
	sort.Slice(errs, func(i, j int) bool {
		return errs[i].ErrCode < errs[j].ErrCode
	})

	last := errs[0].ErrCode
	gaps := []uint16{}

	for i := 1; i < len(errs); i++ {
		if errs[i].ErrCode != last+1 {
			gaps = append(gaps, last)
		}
		last = errs[i].ErrCode
	}

	if len(gaps) > 0 {
		return fmt.Errorf("gaps found in sequences: %v", gaps)
	}

	return nil
}

func checkDupes(errs []server.ErrorsData) error {
	codes := []uint16{}
	highest := uint16(0)
	for _, err := range errs {
		codes = append(codes, err.ErrCode)
		if highest < err.ErrCode {
			highest = err.ErrCode
		}
	}

	codeKeys := make(map[uint16]bool)
	constKeys := make(map[string]bool)

	for _, entry := range errs {
		if _, found := codeKeys[entry.ErrCode]; found {
			return fmt.Errorf("duplicate error code %+v, highest code is %d", entry, highest)
		}

		if _, found := constKeys[entry.Constant]; found {
			return fmt.Errorf("duplicate error constant %+v", entry)
		}

		codeKeys[entry.ErrCode] = true
		constKeys[entry.Constant] = true
	}

	return nil
}

func findTags(d string) []string {
	tags := []string{}
	for _, tag := range tagRe.FindAllStringSubmatch(d, -1) {
		if len(tag) != 2 {
			continue
		}

		tags = append(tags, tag[1])
	}

	sort.Strings(tags)

	return tags
}

func main() {
	ej, err := os.ReadFile("server/errors.json")
	panicIfErr(err)

	errs := []server.ErrorsData{}
	panicIfErr(json.Unmarshal(ej, &errs))

	panicIfErr(checkDupes(errs))
	panicIfErr(checkIncrements(errs))

	sort.Slice(errs, func(i, j int) bool {
		return errs[i].Constant < errs[j].Constant
	})

	t := template.New("errors").Funcs(
		template.FuncMap{
			"inc": func(i int) int { return i + 1 },
			"hasTags": func(d string) bool {
				return strings.Contains(d, "{") && strings.Contains(d, "}")
			},
			"replacerArgsForTags": func(d string) string {
				res := []string{}
				for _, tag := range findTags(d) {
					res = append(res, `"{`+tag+`}"`)
					res = append(res, tag)
				}

				return strings.Join(res, ", ")
			},
			"funcArgsForTags": func(d string) string {
				res := []string{}
				for _, tag := range findTags(d) {
					if tag == "err" {
						res = append(res, "err error")
					} else if tag == "seq" {
						res = append(res, "seq uint64")
					} else {
						res = append(res, fmt.Sprintf("%s interface{}", tag))
					}
				}

				res = append(res, "opts ...ErrorOption")

				return strings.Join(res, ", ")
			},
			"funcNameForConstant": func(c string) string {
				res := ""

				switch {
				case strings.HasSuffix(c, "ErrF"):
					res = fmt.Sprintf("New%sError", strings.TrimSuffix(c, "ErrF"))
				case strings.HasSuffix(c, "Err"):
					res = fmt.Sprintf("New%sError", strings.TrimSuffix(c, "Err"))
				case strings.HasSuffix(c, "ErrorF"):
					res = fmt.Sprintf("New%s", strings.TrimSuffix(c, "F"))
				case strings.HasSuffix(c, "F"):
					res = fmt.Sprintf("New%sError", strings.TrimSuffix(c, "F"))
				default:
					res = fmt.Sprintf("New%s", c)
				}

				if !strings.HasSuffix(res, "Error") {
					res = fmt.Sprintf("%sError", res)
				}

				return res
			},
		})
	p, err := t.Parse(templ)
	panicIfErr(err)

	tf, err := ioutil.TempFile("", "")
	panicIfErr(err)
	defer tf.Close()

	panicIfErr(p.Execute(tf, errs))

	panicIfErr(os.Rename(tf.Name(), "server/jetstream_errors_generated.go"))
	panicIfErr(goFmt("server/jetstream_errors_generated.go"))
}
