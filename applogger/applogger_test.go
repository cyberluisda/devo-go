package applogger

import (
	"bytes"
	"io"
	"regexp"
	"testing"
	"time"
)

func TestLevelComp(t *testing.T) {
	type args struct {
		l Level
		o Level
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			"Equal",
			args{FATAL, FATAL},
			0,
		},
		{
			"Greater",
			args{DEBUG, FATAL},
			1,
		},
		{
			"Less",
			args{FATAL, DEBUG},
			-1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LevelComp(tt.args.l, tt.args.o); got != tt.want {
				t.Errorf("LevelComp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWriterAppLogger_Logf(t *testing.T) {
	buf := &bytes.Buffer{}

	type fields struct {
		Writer     io.Writer
		Level      Level
		AddNow     bool
		TimeLayout string
	}
	type args struct {
		l      Level
		format string
		a      []interface{}
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		outPattern *regexp.Regexp
	}{
		{
			"Level is not enabled",
			fields{
				buf,
				INFO,
				false,
				"",
			},
			args{
				DEBUG,
				"test",
				nil,
			},
			regexp.MustCompile("^$"),
		},
		{
			"Level is enabled",
			fields{
				buf,
				DEBUG,
				false,
				"",
			},
			args{
				DEBUG,
				"test",
				nil,
			},
			regexp.MustCompile("^DEBUG test\n$"),
		},
		{
			"With time format",
			fields{
				buf,
				DEBUG,
				true,
				time.RFC3339,
			},
			args{
				DEBUG,
				"test",
				nil,
			},
			regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[-+]\d{2}:\d{2}) DEBUG test\n$`),
		},
		{
			"With time",
			fields{
				buf,
				DEBUG,
				true,
				"",
			},
			args{
				DEBUG,
				"test",
				nil,
			},
			regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [-+]\d{2}\d{2}.*DEBUG test\n$`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			w := &WriterAppLogger{
				Writer:     tt.fields.Writer,
				Level:      tt.fields.Level,
				AddNow:     tt.fields.AddNow,
				TimeLayout: tt.fields.TimeLayout,
			}
			w.Logf(tt.args.l, tt.args.format, tt.args.a...)

			got := string(buf.Bytes())
			if !tt.outPattern.MatchString(got) {
				t.Errorf("Logf() = '%v', want pattern %v", got, tt.outPattern)
			}
		})
	}
}

func TestWriterAppLogger_Log(t *testing.T) {
	buf := &bytes.Buffer{}

	type fields struct {
		Writer     io.Writer
		Level      Level
		AddNow     bool
		TimeLayout string
	}
	type args struct {
		l Level
		a []interface{}
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		outPattern *regexp.Regexp
	}{
		{
			"Level is not enabled",
			fields{
				buf,
				INFO,
				false,
				"",
			},
			args{
				DEBUG,
				[]interface{}{"test"},
			},
			regexp.MustCompile("^$"),
		},
		{
			"Level is enabled",
			fields{
				buf,
				DEBUG,
				false,
				"",
			},
			args{
				DEBUG,
				[]interface{}{"test"},
			},
			regexp.MustCompile("^DEBUG test\n$"),
		},
		{
			"With time format",
			fields{
				buf,
				DEBUG,
				true,
				time.RFC3339,
			},
			args{
				DEBUG,
				[]interface{}{"test"},
			},
			regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[-+]\d{2}:\d{2} DEBUG test\n$`),
		},
		{
			"With time",
			fields{
				buf,
				DEBUG,
				true,
				"",
			},
			args{
				DEBUG,
				[]interface{}{"test"},
			},
			regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [-+]\d{2}\d{2}.*DEBUG test\n$`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			w := &WriterAppLogger{
				Writer:     tt.fields.Writer,
				Level:      tt.fields.Level,
				AddNow:     tt.fields.AddNow,
				TimeLayout: tt.fields.TimeLayout,
			}
			w.Log(tt.args.l, tt.args.a...)

			got := string(buf.Bytes())
			if !tt.outPattern.MatchString(got) {
				t.Errorf("Log() = '%v', want pattern %v", got, tt.outPattern)
			}
		})
	}
}
