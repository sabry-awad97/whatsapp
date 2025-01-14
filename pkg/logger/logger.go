package logger

import (
	"log"
	"os"
)

// Logger represents a simple logger interface
type Logger interface {
	Info(format string, v ...interface{})
	Error(format string, v ...interface{})
	Debug(format string, v ...interface{})
}

type logger struct {
	infoLogger  *log.Logger
	errorLogger *log.Logger
	debugLogger *log.Logger
}

// New creates a new logger instance
func New() Logger {
	return &logger{
		infoLogger:  log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile),
		errorLogger: log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile),
		debugLogger: log.New(os.Stdout, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}

func (l *logger) Info(format string, v ...interface{}) {
	l.infoLogger.Printf(format, v...)
}

func (l *logger) Error(format string, v ...interface{}) {
	l.errorLogger.Printf(format, v...)
}

func (l *logger) Debug(format string, v ...interface{}) {
	l.debugLogger.Printf(format, v...)
}
