package cc

type Action struct {
	Name        string         `json:"name"`
	Type        string         `json:"type,omitempty"`
	Description string         `json:"desc"`
	Parameters  map[string]any `json:"params"`
}

type Payload struct {
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	Stores     []DataStore            `json:"stores"`
	Inputs     []DataSource           `json:"inputs"`
	Outputs    []DataSource           `json:"outputs"`
	Actions    []Action               `json:"actions"`
}
