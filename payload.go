package wat

type Payload struct {
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	Inputs     []DataSource           `json:"inputs"`
	Outputs    []DataSource           `json:"outputs"`
}
