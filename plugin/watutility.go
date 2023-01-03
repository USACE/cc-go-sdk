package plugin

// EventConfiguration is a simple structure to support consistency in wat plugins regarding the usage of seeds for natural variability and knowledge uncertainty and realization numbers for indexing
type EventConfiguration struct {
	RealizationNumber int                `json:"realization_number"`
	Seeds             map[string]SeedSet `json:"seeds"` //pluginName-modelname as the general key convention?
}

// SeedSet a seed set is a struct to define a natural variability and a knowledge uncertainty
type SeedSet struct {
	EventSeed       int64 `json:"event_seed"`
	RealizationSeed int64 `json:"realization_seed"`
}
