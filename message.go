package wat

import (
	"fmt"
	"runtime"
	"time"
)

type Level uint8

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
	FATAL
	PANIC
	DISABLED
)

func (l Level) String() string {
	switch l {
	case INFO:
		return "some Information"
	case WARN:
		return "a Warning"
	case ERROR:
		return "an Error"
	case DEBUG:
		return "a Debug statement"
	case FATAL:
		return "a Fatal message"
	case PANIC:
		return "a Panic'ed state"
	case DISABLED:
		return ""
	default:
		return "Unknown Level"
	}
}

type Logger struct {
	Level //i believe this will be global to the container each container having its own possible level (and wat having its own level too.)
}

var logger = Logger{
	Level: INFO,
}

type Status string

const (
	COMPUTING Status = "Computing"
	FAILED    Status = "Failed"
	SUCCEEDED Status = "Succeeded"
)

type Message struct {
	Status    Status `json:"status,omitempty"`
	Progress  int8   `json:"progress,omitempty"`
	Level     Level  `json:"level"`
	Message   string `json:"message"`
	Sender    string `json:"sender,omitempty"`
	PayloadId string `json:"payload_id"`
	timeStamp time.Time
}

// write is just a placeholder for however we intend to implement logging by the sdk
func (l Logger) write(log Message) (n int, err error) {
	log.timeStamp = time.Now()

	sender := ""
	if log.Sender == "" {
		sender = "Unknown Sender"
	} else {
		sender = log.Sender
	}
	if l.Level == DEBUG {
		pc, file, line, _ := runtime.Caller(2)
		funcName := runtime.FuncForPC(pc).Name()
		fmt.Printf("%v issues %v at %v from file %v on line %v in method name %v\n\t%v\n", sender, log.Level.String(), log.timeStamp, file, line, funcName, log.Message)
	} else {
		if log.Level >= ERROR {
			pc, file, line, _ := runtime.Caller(2)
			funcName := runtime.FuncForPC(pc).Name()
			fmt.Printf("%v issues %v at %v from file %v on line %v in method name %v\n\t%v\n", sender, log.Level.String(), log.timeStamp, file, line, funcName, log.Message)
		}
		fmt.Printf("%v issues %v at %v\n\t%v\n", sender, log.Level.String(), log.timeStamp, log.Message)
	}
	return 0, nil
}

// SetLogLevel accepts a Level for messages.
func SetLogLevel(logLevel Level) {
	logger.Level = logLevel
}

// Log accepts a message and manages the writing of messages that have levels that exceed or equal the instance level.
func Log(message Message) {
	if logger.Level <= message.Level {
		logger.write(message)
	}
}
