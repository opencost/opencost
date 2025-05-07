package log

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func TestGetLogger(t *testing.T) {
	initialLogger := GetLogger()
	if initialLogger == nil {
		t.Error("GetLogger() returned nil")
	}

	secondLogger := GetLogger()
	if initialLogger != secondLogger {
		t.Error("GetLogger() returned different loggers on subsequent calls")
	}
}

func TestSetLogger(t *testing.T) {
	var buf bytes.Buffer
	newLogger := zerolog.New(&buf).With().Str("test", "value").Logger()
	SetLogger(&newLogger)

	// Log a message using the global logger
	Infof("Test message")

	// Parse the logged message
	loggedData := parseLogMessage(t, buf.String())

	// Check if the "test" field is present in the logged message
	if value, exists := loggedData["test"]; !exists || value != "value" {
		t.Error("SetLogger() did not set the logger with expected context")
	}
}

func TestLoggerConsistency(t *testing.T) {
	var buf bytes.Buffer
	newLogger := zerolog.New(&buf).With().Str("test", "consistency").Logger()
	SetLogger(&newLogger)

	// Log a message using the global logger
	Infof("Consistency test message")

	// Parse the logged message
	loggedData := parseLogMessage(t, buf.String())

	// Check if the "test" field is present in the logged message
	if value, exists := loggedData["test"]; !exists || value != "consistency" {
		t.Error("Logger inconsistency: Updated logger does not have expected context")
	}
}

func parseLogMessage(t *testing.T, logMessage string) map[string]interface{} {
	var loggedData map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(logMessage)), &loggedData); err != nil {
		t.Fatalf("Failed to parse logged message: %v", err)
	}
	return loggedData
}

func TestInitLogging(t *testing.T) {
	// Save original logger and restore it after tests
	originalLogger := log.Logger
	defer func() {
		log.Logger = originalLogger
	}()

	// Test cases
	tests := []struct {
		name                   string
		format                 string
		level                  string
		disableColor           bool
		showLogLevelSetMessage bool
		expectedLevel          zerolog.Level
	}{
		{
			name:                   "default settings",
			format:                 "pretty",
			level:                  "info",
			disableColor:           false,
			showLogLevelSetMessage: true,
			expectedLevel:          zerolog.InfoLevel,
		},
		{
			name:                   "json format",
			format:                 "json",
			level:                  "debug",
			disableColor:           false,
			showLogLevelSetMessage: false,
			expectedLevel:          zerolog.DebugLevel,
		},
		{
			name:                   "invalid level",
			format:                 "pretty",
			level:                  "invalid",
			disableColor:           false,
			showLogLevelSetMessage: false,
			expectedLevel:          zerolog.InfoLevel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Set(flagFormat, tt.format)
			viper.Set(flagLevel, tt.level)
			viper.Set(flagDisableColor, tt.disableColor)

			InitLogging(tt.showLogLevelSetMessage)

			if zerolog.GlobalLevel() != tt.expectedLevel {
				t.Errorf("expected level %v, got %v", tt.expectedLevel, zerolog.GlobalLevel())
			}
		})
	}
}

func TestLogLevelManagement(t *testing.T) {
	// Save original logger and restore it after tests
	originalLogger := log.Logger
	defer func() {
		log.Logger = originalLogger
	}()

	tests := []struct {
		name        string
		level       string
		expectError bool
	}{
		{"valid level - debug", "debug", false},
		{"valid level - info", "info", false},
		{"valid level - warn", "warn", false},
		{"valid level - error", "error", false},
		{"invalid level", "invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SetLogLevel(tt.level)
			if (err != nil) != tt.expectError {
				t.Errorf("SetLogLevel() error = %v, expectError %v", err, tt.expectError)
			}

			if !tt.expectError {
				currentLevel := GetLogLevel()
				if currentLevel != tt.level {
					t.Errorf("GetLogLevel() = %v, want %v", currentLevel, tt.level)
				}
			}
		})
	}
}

func TestLoggingFunctions(t *testing.T) {
	// Create a buffer to capture log output
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	SetLogger(&logger)

	// Set log level to trace to ensure all messages are captured
	zerolog.SetGlobalLevel(zerolog.TraceLevel)

	tests := []struct {
		name     string
		logFunc  func(string)
		logMsg   string
		expected string
	}{
		{"Error", Error, "test error", "error"},
		{"Warn", Warn, "test warning", "warn"},
		{"Info", Info, "test info", "info"},
		{"Debug", Debug, "test debug", "debug"},
		{"Trace", Trace, "test trace", "trace"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			tt.logFunc(tt.logMsg)
			output := buf.String()
			if !strings.Contains(output, tt.expected) {
				t.Errorf("expected log level %s in output, got: %s", tt.expected, output)
			}
		})
	}

	// Test Fatal separately since it calls os.Exit
	t.Run("Fatal", func(t *testing.T) {
		// Create a new buffer for Fatal test
		var fatalBuf bytes.Buffer
		fatalLogger := zerolog.New(&fatalBuf)
		SetLogger(&fatalLogger)

		// We can't actually test the Fatal function since it calls os.Exit
		// Instead, we'll verify that the Fatal function exists by checking its type
		// and that it can be assigned to a variable of the correct type
		var fatalFunc func(string) = Fatal
		if fatalFunc == nil {
			t.Error("Fatal function is nil")
		}
	})
}

func TestDedupedLogging(t *testing.T) {
	// Create a buffer to capture log output
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	SetLogger(&logger)

	tests := []struct {
		name         string
		logFunc      func(int, string, ...interface{})
		logTypeLimit int
		format       string
		args         []interface{}
	}{
		{
			name:         "DedupedErrorf",
			logFunc:      DedupedErrorf,
			logTypeLimit: 3,
			format:       "test error %d",
			args:         []interface{}{1},
		},
		{
			name:         "DedupedWarningf",
			logFunc:      DedupedWarningf,
			logTypeLimit: 2,
			format:       "test warning %d",
			args:         []interface{}{1},
		},
		{
			name:         "DedupedInfof",
			logFunc:      DedupedInfof,
			logTypeLimit: 4,
			format:       "test info %d",
			args:         []interface{}{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			// Log up to the limit
			for i := 0; i < tt.logTypeLimit+1; i++ {
				tt.logFunc(tt.logTypeLimit, tt.format, tt.args...)
			}

			output := buf.String()
			// Count occurrences of the exact log message (excluding the suppression message)
			exactMsg := fmt.Sprintf(tt.format, tt.args...)
			count := strings.Count(output, exactMsg) - 1 // Subtract 1 for the suppression message

			if count != tt.logTypeLimit {
				t.Errorf("expected %d occurrences of log message, got %d", tt.logTypeLimit, count)
			}

			// Verify suppression message
			suppressionMsg := fmt.Sprintf("%s logged %d times: suppressing future logs", exactMsg, tt.logTypeLimit)
			if !strings.Contains(output, suppressionMsg) {
				t.Error("expected suppression message in output")
			}
		})
	}
}

func TestProfiling(t *testing.T) {
	// Create a buffer to capture log output
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	SetLogger(&logger)

	t.Run("Profilef", func(t *testing.T) {
		buf.Reset()
		Profilef("test profile %d", 1)
		output := buf.String()
		if !strings.Contains(output, "[Profiler]") {
			t.Error("expected [Profiler] in output")
		}
	})

	t.Run("Profile", func(t *testing.T) {
		buf.Reset()
		start := time.Now()
		time.Sleep(10 * time.Millisecond) // Ensure some time has passed
		Profile(start, "test operation")
		output := buf.String()
		if !strings.Contains(output, "test operation") {
			t.Error("expected operation name in output")
		}
	})

	t.Run("ProfileWithThreshold", func(t *testing.T) {
		buf.Reset()
		start := time.Now()
		time.Sleep(10 * time.Millisecond)
		ProfileWithThreshold(start, 5*time.Millisecond, "test operation")
		output := buf.String()
		if !strings.Contains(output, "test operation") {
			t.Error("expected operation name in output")
		}

		// Test with threshold not exceeded
		buf.Reset()
		start = time.Now()
		ProfileWithThreshold(start, 100*time.Millisecond, "test operation")
		output = buf.String()
		if output != "" {
			t.Error("expected no output when threshold not exceeded")
		}
	})
}

func TestLoggerManagement(t *testing.T) {
	// Save original logger and restore it after tests
	originalLogger := log.Logger
	defer func() {
		log.Logger = originalLogger
	}()

	// Create a new logger with a unique field to identify it
	var buf bytes.Buffer
	newLogger := zerolog.New(&buf).With().Str("test_id", "unique_logger").Logger()

	// Test GetLogger
	currentLogger := GetLogger()
	if currentLogger == nil {
		t.Error("GetLogger() returned nil")
	}

	// Test SetLogger
	SetLogger(&newLogger)

	// Log a message and verify it contains our unique identifier
	Info("test message")
	output := buf.String()
	if !strings.Contains(output, "unique_logger") {
		t.Error("SetLogger() did not set the logger correctly")
	}
}
