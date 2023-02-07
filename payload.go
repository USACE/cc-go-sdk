package cc

type Payload struct {
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	Stores     []DataStore            `json:"stores"`
	Inputs     []DataSource           `json:"inputs"`
	Outputs    []DataSource           `json:"outputs"`
}
