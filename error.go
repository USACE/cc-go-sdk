package wat

type ErrorLevel uint8

const (
	DEBUG ErrorLevel = iota
	INFO
	WARN
	ERROR
	FATAL
	PANIC
	DISABLED
)

func (l ErrorLevel) String() string {
	switch l {
	case INFO:
		return "some information"
	case WARN:
		return "a warning"
	case ERROR:
		return "an error"
	case DEBUG:
		return "a debug statement"
	case FATAL:
		return "a fatal message"
	case PANIC:
		return "a panic'ed state"
	case DISABLED:
		return ""
	default:
		return "unknown level"
	}
}

type Error struct {
	ErrorLevel ErrorLevel `json:"errorlevel"`
	Error      string     `json:"error"`
}
