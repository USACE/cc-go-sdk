package wat

import (
	"fmt"
	"runtime"
	"time"
)

type Logger struct {
	ErrorFilter ErrorLevel //i believe this will be global to the container each container having its own possible level (and wat having its own level too.)
}

var logger = Logger{
	ErrorFilter: INFO,
}

type Status string

const (
	COMPUTING Status = "Computing"
	FAILED    Status = "Failed"
	SUCCEEDED Status = "Succeeded"
)

type Message struct {
	Message   string `json:"message"`
	Sender    string `json:"sender,omitempty"`
	timeStamp time.Time
}

// write is just a placeholder for however we intend to implement logging by the sdk
func (l Logger) writeError(log Error) (n int, err error) {
	log.timeStamp = time.Now()

	sender := ""
	if log.Sender == "" {
		sender = "Unknown Sender"
	} else {
		sender = log.Sender
	}
	if l.ErrorFilter == DEBUG {
		pc, file, line, _ := runtime.Caller(2)
		funcName := runtime.FuncForPC(pc).Name()
		fmt.Printf("%v issues %v at %v from file %v on line %v in method name %v\n\t%v\n", sender, log.ErrorLevel.String(), log.timeStamp, file, line, funcName, log.Error)
	} else {
		if log.ErrorLevel >= ERROR {
			pc, file, line, _ := runtime.Caller(2)
			funcName := runtime.FuncForPC(pc).Name()
			fmt.Printf("%v issues %v at %v from file %v on line %v in method name %v\n\t%v\n", sender, log.ErrorLevel.String(), log.timeStamp, file, line, funcName, log.Error)
		}
		fmt.Printf("%v issues %v at %v\n\t%v\n", sender, log.ErrorLevel.String(), log.timeStamp, log.Error)
	}
	return 0, nil
}

func (l Logger) write(log Message) (n int, err error) {
	log.timeStamp = time.Now()
	fmt.Printf("%v:%v\n\t%v\n", log.Sender, log.timeStamp, log.Message)
	return 0, nil
}

// SetLogLevel accepts a Level for messages.
func SetErrorFilter(logLevel ErrorLevel) {
	logger.ErrorFilter = logLevel
}

// Log accepts a message and manages the writing of messages that have levels that exceed or equal the instance level.
func LogMessage(message Message) {
	//this could go to a redis cache, sqs, or just to log files for cloud watch to manage. The point is a single struct and a single endpoint to manage consistent logging across plugins.
	logger.write(message)
}
func LogError(err Error) {
	if logger.ErrorFilter <= err.ErrorLevel {
		//this could go to a redis cache, sqs, or just to log files for cloud watch to manage. The point is a single struct and a single endpoint to manage consistent logging across plugins.
		logger.writeError(err)
	}
}
