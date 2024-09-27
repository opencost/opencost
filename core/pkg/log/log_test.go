package log

import (
	"testing"

	"github.com/rs/zerolog"
)

func TestGetLogger(t *testing.T) {
	// Get the initial logger
	initialLogger := GetLogger()

	if initialLogger == nil {
		t.Error("GetLogger() returned nil")
	}

	// Ensure that calling GetLogger() multiple times returns the same logger
	secondLogger := GetLogger()
	if initialLogger != secondLogger {
		t.Error("GetLogger() returned different loggers on subsequent calls")
	}
}

func TestSetLogger(t *testing.T) {
	// Create a new logger
	newLogger := zerolog.New(nil).With().Timestamp().Logger()

	// Set the new logger
	SetLogger(&newLogger)

	// Get the logger and check if it's the one we set
	retrievedLogger := GetLogger()
	if retrievedLogger != &newLogger {
		t.Error("SetLogger() did not set the logger correctly")
	}

	// Reset the logger to its original state (assuming there's a default logger)
	defaultLogger := zerolog.New(nil).With().Timestamp().Logger()
	SetLogger(&defaultLogger)
}

func TestLoggerConsistency(t *testing.T) {
	// Get the initial logger
	initialLogger := GetLogger()

	// Create a new logger
	newLogger := zerolog.New(nil).With().Timestamp().Logger()

	// Set the new logger
	SetLogger(&newLogger)

	// Get the logger again
	updatedLogger := GetLogger()

	// Check if the updated logger is the one we set
	if updatedLogger != &newLogger {
		t.Error("Logger inconsistency: GetLogger() did not return the logger set by SetLogger()")
	}

	// Check that the initial logger is different from the updated logger
	if initialLogger == updatedLogger {
		t.Error("Logger inconsistency: Initial logger should be different from updated logger")
	}

	// Reset the logger to its original state (assuming there's a default logger)
	defaultLogger := zerolog.New(nil).With().Timestamp().Logger()
	SetLogger(&defaultLogger)
}
