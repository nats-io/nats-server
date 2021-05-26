// +build ignore

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"sort"
	"text/template"
)

var templ = `
// Generated code, do not edit. See errors.json and run go generate to update

package server

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
		{{ .Constant }}: {Code: {{ .Code }},ErrCode: {{ .ErrCode }},Description: {{ .Description | printf "%q" }},{{- if .Help }}Help: {{ .Help | printf "%q" }},{{- end }}{{- if .URL }}URL: {{ .URL | printf "%q" }},{{- end }}	},{{- end }}
	}

{{- range $i, $error := . }}
{{- if .Deprecates }}
// {{ .Deprecates }} Deprecated by {{ .Constant }} ApiError, use IsNatsError() for comparisons
{{ .Deprecates }} = ApiErrors[{{ .Constant }}]
{{- end }}
{{- end }}
)
`

func panicIfErr(err error) {
	if err == nil {
		return
	}
	panic(err)
}

type Errors struct {
	Constant    string `json:"constant"`
	Code        int    `json:"code"`
	ErrCode     int    `json:"error_code"`
	Description string `json:"description"`
	Comment     string `json:"comment"`
	Help        string `json:"help"`
	URL         string `json:"url"`
	Deprecates  string `json:"deprecates"`
}

func goFmt(file string) error {
	c := exec.Command("go", "fmt", file)
	out, err := c.CombinedOutput()
	if err != nil {
		log.Printf("go fmt failed: %s", string(out))
	}

	return err
}

func checkDupes(errs []Errors) error {
	codes := []int{}
	highest := 0
	for _, err := range errs {
		codes = append(codes, err.ErrCode)
		if highest < err.ErrCode {
			highest = err.ErrCode
		}
	}

	codeKeys := make(map[int]bool)
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

func main() {
	ej, err := os.ReadFile("server/errors.json")
	panicIfErr(err)

	errs := []Errors{}
	panicIfErr(json.Unmarshal(ej, &errs))
	panicIfErr(checkDupes(errs))

	sort.Slice(errs, func(i, j int) bool {
		return errs[i].Constant < errs[j].Constant
	})

	t := template.New("errors").Funcs(template.FuncMap{"inc": func(i int) int { return i + 1 }})
	p, err := t.Parse(templ)
	panicIfErr(err)

	tf, err := ioutil.TempFile("", "")
	panicIfErr(err)
	defer tf.Close()

	panicIfErr(p.Execute(tf, errs))

	panicIfErr(os.Rename(tf.Name(), "server/jetstream_errors_generated.go"))
	panicIfErr(goFmt("server/jetstream_errors_generated.go"))
}
