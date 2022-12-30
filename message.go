package wat

type Message struct {
	Message string `json:"message"`
	Sender  string `json:"sender,omitempty"`
}
