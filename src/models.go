package main

import "time"

type User struct {
	DiscordId          float64 `json:"discord_id"`
	BungieMembershipId float64 `json:"bungie_membership_id"`
	Ada1               bool    `json:"ada_1"`
	CreatedAt          string  `json:"created_at"`
}

type Telemetry struct {
	JobId          string     `json:"id,omitempty"`
	StartTime      *time.Time `json:"start_time,omitempty"`
	WorkerCount    int64      `json:"worker_count,omitempty"`
	TotalUsers     int64      `json:"total_users,omitempty"`
	ProcessedUsers int64      `json:"processed_users,omitempty"`
	ProcessingRate float64    `json:"processing_rate,omitempty"`
	ExecutionTime  float64    `json:"execution_time,omitempty"`
}
