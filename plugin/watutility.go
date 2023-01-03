package plugin

type EventConfiguration struct {
	RealizationNumber int       `json:"realization_number"`
	Seeds             []SeedSet `json:"seeds"`
}
type SeedSet struct {
	Identifier      string `json:"identifier"`
	EventSeed       int64  `json:"event_seed"`
	RealizationSeed int64  `json:"realization_seed"`
}
