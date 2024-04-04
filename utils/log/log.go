package log

import (
	"log"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.SugaredLogger

// Instantiate creates a new structured logger instance
func Instantiate() error {

	format := os.Getenv("LOG_FORMAT")
	if format == "" {
		format = "console"
	}
	log.Default().Print("Log format: ", format)
	zLogger, err := zap.Config{
		Encoding:          format,
		Level:             zap.NewAtomicLevelAt(zapcore.InfoLevel),
		DisableCaller:     false,
		DisableStacktrace: false,
		OutputPaths:       []string{"stdout"},
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:      "time",
			EncodeTime:   zapcore.RFC3339TimeEncoder,
			LevelKey:     "level",
			EncodeLevel:  zapcore.CapitalColorLevelEncoder,
			MessageKey:   "message",
			CallerKey:    "caller",
			EncodeCaller: zapcore.ShortCallerEncoder,
		},
	}.Build()
	if err != nil {
		return err
	}
	logger = zLogger.Sugar()
	return nil
}

func Info(msg string, args ...interface{}) {
	logger.Infof(msg, args...)
}

func Debug(msg string, args ...interface{}) {
	logger.Debugf(msg, args...)
}

func Warn(msg string, args ...interface{}) {
	logger.Warnf(msg, args...)
}

func Error(msg string, args ...interface{}) {
	logger.Errorf(msg, args...)
}

func Fatal(msg string, args ...interface{}) {
	logger.Fatalf(msg, args...)
}
