package cc

type Status string

const (
	COMPUTING Status = "Computing"
	FAILED    Status = "Failed"
	SUCCEEDED Status = "Succeeded"
)

type StatusReport struct {
	Status   Status `json:"status"`
	Progress int    `json:"progress, omitempty"`
}
