package log

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// TODO for deduped functions, if timeLogged > logTypeLimit, should we log once
// every... 100 (?) times so we don't lose track entirely?

// concurrency-safe counter
var ctr = newCounter()

const (
	flagFormat       = "log-format"
	flagLevel        = "log-level"
	flagDisableColor = "disable-log-color"
)

// Options contains settings for logging initialization.
type Options struct {
	// DisableLogLevelReportMessage prevents logging initialization from
	// reporting the log level once complete. This is useful for applications
	// like CLI tools, where a log message stating "log level is set to X" is
	// too much noise.
	//
	// This defaults to false because most users of logging are services, which
	// benefit from extra reassurance about the log level.
	DisableLogLevelReportMessage bool

	// LevelPFlag is an optional flag that will be used as the first source
	// of truth for the log level.
	// If set to nil, will not do anything.
	LevelPFlag *pflag.Flag
	// FormatPFlag is an optional flag that will be used as the first source
	// of truth for the log format.
	// If set to nil, will not do anything.
	FormatPFlag *pflag.Flag
	// DisableColorPFlag is an optional flag that will be used as the first
	// source of truth for whether log colors should be disabled.
	// If set to nil, will not do anything.
	DisableColorPFlag *pflag.Flag
}

// InitLoggingWithOptions initializes logging with custom behavior. See the
// options struct for explanations of each option.
func InitLoggingWithOptions(o Options) {
	// Bind the flags to our expected viper strings. We drop the error because
	// we're okay with failing to bind a nil flag and proceeding.
	viper.BindPFlag(flagLevel, o.LevelPFlag)
	viper.BindPFlag(flagFormat, o.FormatPFlag)
	viper.BindPFlag(flagDisableColor, o.DisableColorPFlag)

	zerolog.TimeFieldFormat = time.RFC3339Nano
	// Default to using pretty formatting
	if strings.ToLower(viper.GetString(flagFormat)) != "json" {
		disableColor := viper.GetBool(flagDisableColor)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano, NoColor: disableColor})
	}

	level, err := zerolog.ParseLevel(viper.GetString(flagLevel))
	if err != nil {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		log.Warn().Msg("Error parsing log-level, setting level to 'info'")
		return
	}
	zerolog.SetGlobalLevel(level)

	if !o.DisableLogLevelReportMessage {
		log.Log().Msgf("Log level set to %v", level)
	}
}

// InitLogging initializes logging with standard behavior.
func InitLogging() {
	InitLoggingWithOptions(Options{})
}

func Errorf(format string, a ...interface{}) {
	log.Error().Msgf(format, a...)
}

func DedupedErrorf(logTypeLimit int, format string, a ...interface{}) {
	timesLogged := ctr.increment(format)

	if timesLogged < logTypeLimit {
		Errorf(format, a...)
	} else if timesLogged == logTypeLimit {
		Errorf(format, a...)
		Infof("%s logged %d times: suppressing future logs", format, logTypeLimit)
	}
}

func Warnf(format string, a ...interface{}) {
	log.Warn().Msgf(format, a...)
}

func DedupedWarningf(logTypeLimit int, format string, a ...interface{}) {
	timesLogged := ctr.increment(format)

	if timesLogged < logTypeLimit {
		Warnf(format, a...)
	} else if timesLogged == logTypeLimit {
		Warnf(format, a...)
		Infof("%s logged %d times: suppressing future logs", format, logTypeLimit)
	}
}

func Info(msg string) {
	log.Info().Msg(msg)
}

func Infof(format string, a ...interface{}) {
	log.Info().Msgf(format, a...)
}

func DedupedInfof(logTypeLimit int, format string, a ...interface{}) {
	timesLogged := ctr.increment(format)

	if timesLogged < logTypeLimit {
		Infof(format, a...)
	} else if timesLogged == logTypeLimit {
		Infof(format, a...)
		Infof("%s logged %d times: suppressing future logs", format, logTypeLimit)
	}
}

func Profilef(format string, a ...interface{}) {
	log.Info().Msgf(fmt.Sprintf("[Profiler] %s", format), a...)
}

func Debug(msg string) {
	log.Debug().Msg(msg)
}

func Debugf(format string, a ...interface{}) {
	log.Debug().Msgf(format, a...)
}

func Fatalf(format string, a ...interface{}) {
	log.Fatal().Msgf(format, a...)
}

func Profile(start time.Time, name string) {
	elapsed := time.Since(start)
	Profilef("%s: %s", elapsed, name)
}

func ProfileWithThreshold(start time.Time, threshold time.Duration, name string) {
	elapsed := time.Since(start)
	if elapsed > threshold {
		Profilef("%s: %s", elapsed, name)
	}
}
