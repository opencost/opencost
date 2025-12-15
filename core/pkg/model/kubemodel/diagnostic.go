package kubemodel

import (
	"fmt"
	"time"
)

// @bingen:generate:DiagnosticLevel
type DiagnosticLevel int

const (
	DiagnosticLevelTrace DiagnosticLevel = iota
	DiagnosticLevelDebug
	DiagnosticLevelInfo
	DiagnosticLevelWarning
	DiagnosticLevelError
)

const DefaultDiagnosticLevel = DiagnosticLevelInfo

// @bingen:generate:Diagnostic
type Diagnostic struct {
	Timestamp time.Time         `json:"timestamp"`         // @bingen:field[version=1]
	Level     DiagnosticLevel   `json:"level"`             // @bingen:field[version=1]
	Message   string            `json:"message"`           // @bingen:field[version=1]
	Details   map[string]string `json:"details,omitempty"` // @bingen:field[version=1]
}

func (kms *KubeModelSet) RegisterDiagnostic(d Diagnostic) {
	kms.Metadata.Diagnostics = append(kms.Metadata.Diagnostics, d)
}

func (kms *KubeModelSet) GetErrors() []Diagnostic {
	ds := []Diagnostic{}

	for _, d := range kms.Metadata.Diagnostics {
		if d.Level == DiagnosticLevelError {
			ds = append(ds, d)
		}
	}

	return ds
}

func (kms *KubeModelSet) Error(err error) {
	kms.RegisterDiagnostic(Diagnostic{
		Timestamp: time.Now().UTC(),
		Level:     DiagnosticLevelError,
		Message:   err.Error(),
	})
}

func (kms *KubeModelSet) GetWarnings() []Diagnostic {
	ds := []Diagnostic{}

	for _, d := range kms.Metadata.Diagnostics {
		if d.Level == DiagnosticLevelWarning {
			ds = append(ds, d)
		}
	}

	return ds
}

func (kms *KubeModelSet) Warnf(msg string, a ...any) {
	kms.Warn(fmt.Sprintf(msg, a...))
}

func (kms *KubeModelSet) Warn(msg string) {
	if kms.Metadata.DiagnosticLevel > DiagnosticLevelWarning {
		return
	}

	kms.RegisterDiagnostic(Diagnostic{
		Timestamp: time.Now().UTC(),
		Level:     DiagnosticLevelWarning,
		Message:   msg,
	})
}

func (kms *KubeModelSet) GetInfos() []Diagnostic {
	ds := []Diagnostic{}

	for _, d := range kms.Metadata.Diagnostics {
		if d.Level == DiagnosticLevelInfo {
			ds = append(ds, d)
		}
	}

	return ds
}

func (kms *KubeModelSet) Infof(msg string, a ...any) {
	kms.Info(fmt.Sprintf(msg, a...))
}

func (kms *KubeModelSet) Info(msg string) {
	if kms.Metadata.DiagnosticLevel > DiagnosticLevelInfo {
		return
	}

	kms.RegisterDiagnostic(Diagnostic{
		Timestamp: time.Now().UTC(),
		Level:     DiagnosticLevelInfo,
		Message:   msg,
	})
}

func (kms *KubeModelSet) GetDebugs() []Diagnostic {
	ds := []Diagnostic{}

	for _, d := range kms.Metadata.Diagnostics {
		if d.Level == DiagnosticLevelDebug {
			ds = append(ds, d)
		}
	}

	return ds
}

func (kms *KubeModelSet) Debugf(msg string, a ...any) {
	kms.Debug(fmt.Sprintf(msg, a...))
}

func (kms *KubeModelSet) Debug(msg string) {
	if kms.Metadata.DiagnosticLevel > DiagnosticLevelDebug {
		return
	}

	kms.RegisterDiagnostic(Diagnostic{
		Timestamp: time.Now().UTC(),
		Level:     DiagnosticLevelDebug,
		Message:   msg,
	})
}

func (kms *KubeModelSet) GetTraces() []Diagnostic {
	ds := []Diagnostic{}

	for _, d := range kms.Metadata.Diagnostics {
		if d.Level == DiagnosticLevelTrace {
			ds = append(ds, d)
		}
	}

	return ds
}

func (kms *KubeModelSet) Tracef(msg string, a ...any) {
	kms.Trace(fmt.Sprintf(msg, a...))
}

func (kms *KubeModelSet) Trace(msg string) {
	if kms.Metadata.DiagnosticLevel > DiagnosticLevelTrace {
		return
	}

	kms.RegisterDiagnostic(Diagnostic{
		Timestamp: time.Now().UTC(),
		Level:     DiagnosticLevelTrace,
		Message:   msg,
	})
}
