package wat

type Payload struct {
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	Stores     []DataStoreDef         `json:"stores"`
	Inputs     []DataSource           `json:"inputs"`
	Outputs    []DataSource           `json:"outputs"`
}
