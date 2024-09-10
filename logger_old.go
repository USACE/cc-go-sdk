package cc

import (
	"fmt"
	"runtime"
	"time"
)

type Logger struct {
	ErrorFilter ErrorLevel //i believe this will be global to the container each container having its own possible level (and compute having its own level too.)
	Sender      string
}

// write is just a placeholder for however we intend to implement logging by the sdk
func (l Logger) writeError(log Error) error {
	if l.ErrorFilter == DEBUG {
		pc, file, line, _ := runtime.Caller(2)
		funcName := runtime.FuncForPC(pc).Name()
		fmt.Printf("%v issues %v at %v from file %v on line %v in method name %v\n\t%v\n", l.Sender, log.ErrorLevel.String(), time.Now(), file, line, funcName, log.Error)
	} else {
		if log.ErrorLevel >= ERROR {
			pc, file, line, _ := runtime.Caller(2)
			funcName := runtime.FuncForPC(pc).Name()
			fmt.Printf("%v issues %v at %v from file %v on line %v in method name %v\n\t%v\n", l.Sender, log.ErrorLevel.String(), time.Now(), file, line, funcName, log.Error)
		}
		fmt.Printf("%v issues %v at %v\n\t%v\n", l.Sender, log.ErrorLevel.String(), time.Now(), log.Error)
	}
	return nil
}

func (l Logger) write(log Message) error {
	fmt.Printf("%v:%v\n\t%v\n", l.Sender, time.Now(), log.Message)
	return nil
}
func (l Logger) reportStatus(status StatusReport) error {
	fmt.Printf("%v:%v:%v\n\t%v percent complete\n", l.Sender, status.Status, time.Now(), status.Progress) //can we make sender an environment variable?
	return nil
}

// SetLogLevel accepts a Level for messages.
func (l *Logger) SetErrorFilter(logLevel ErrorLevel) {
	l.ErrorFilter = logLevel
}

// Log accepts a message and manages the writing of messages that have levels that exceed or equal the instance level.
func (l Logger) LogMessage(message Message) {
	//this could go to a redis cache, sqs, or just to log files for cloud watch to manage. The point is a single struct and a single endpoint to manage consistent logging across plugins.
	l.write(message)
}
func (l Logger) LogError(err Error) {
	if l.ErrorFilter <= err.ErrorLevel {
		//this could go to a redis cache, sqs, or just to log files for cloud watch to manage. The point is a single struct and a single endpoint to manage consistent logging across plugins.
		l.writeError(err)
	}
}
func (l Logger) ReportProgress(status StatusReport) {
	l.reportStatus(status)
}
