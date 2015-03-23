package logger

import (
	"log"
	"os"
)

const (
	Fatal int = iota
	Error
	Warn
	Info
	Debug
)

var levelNames = []string{
	"FATAL",
	"ERROR",
	"WARN",
	"INFO",
	"DEBUG",
}

type Logger struct {
	level  int
	prefix string
}

func NewLogger(prefix string) Logger {
	return Logger{
		prefix: prefix,
		level:  Info,
	}
}

func (l *Logger) SetLevel(level int) {
	l.level = level
}

func (l *Logger) Info(msg string) {
	l.writeLog(msg, Info)
}

func (l *Logger) Debug(msg string) {
	l.writeLog(msg, Debug)
}

func (l *Logger) Warn(msg string) {
	l.writeLog(msg, Warn)
}

func (l *Logger) Error(err error) {
	l.writeLog(err.Error(), Error)
}

func (l *Logger) Fatal(msg string) {
	l.writeLog(msg, Debug)
	os.Exit(1)
}

func (l *Logger) enabled(level int) bool {
	return level <= l.level
}

func (l *Logger) writeLog(msg string, level int) {
	if l.enabled(level) {
		log.Printf("[%s] [%s] %s\n", levelNames[level], l.prefix, msg)
	}
}
