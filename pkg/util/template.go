package util

import (
	"bytes"
	"text/template"
)

type TemplateBuffer struct {
	buffer *bytes.Buffer
}

func NewTemplateBuffer() *TemplateBuffer {
	return &TemplateBuffer{
		buffer: new(bytes.Buffer),
	}
}

func (tb *TemplateBuffer) Render(str string, values map[string]interface{}) string {
	tb.buffer.Reset()
	tpl := template.Must(template.New("default").Delims("<<", ">>").Parse(str))
	if err := tpl.Execute(tb.buffer, values); err != nil {
		panic(err)
	}
	return tb.buffer.String()
}
