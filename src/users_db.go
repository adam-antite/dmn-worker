package main

import (
	"encoding/json"
	supa "github.com/nedpals/supabase-go"
	"log"
	"time"
)

type DatabaseClient interface {
	GetAllUsers() []User
	InsertTelemetry(telem Telemetry) error
	UpdateTelemetry(telem Telemetry) error
}

type SupabaseClient struct {
	supa *supa.Client
}

func NewSupabaseClient(supabaseUrl string, supabaseServiceRoleKey string) *SupabaseClient {
	client := supa.CreateClient(supabaseUrl, supabaseServiceRoleKey)
	return &SupabaseClient{client}
}

func (s SupabaseClient) GetAllUsers() []User {
	start := time.Now()

	var results []map[string]interface{}
	var users []User

	log.Println("scanning users table...")

	err := supabase.DB.From("users").Select("*").Execute(&results)
	if err != nil {
		log.Println("error querying users table: ", err)
		panic(err)
	}

	for _, data := range results {
		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Println("error marshalling user data into json")
			panic(err)
		}

		user := User{}
		err = json.Unmarshal(jsonData, &user)
		if err != nil {
			log.Println("error unmarshalling user data into user struct")
			panic(err)
		}

		users = append(users, user)
	}

	userCount = len(results)
	executionTime := time.Since(start)
	log.Printf("finished scanning after %s", executionTime.Truncate(time.Millisecond))
	log.Printf("user count: %d", userCount)

	return users
}

func (s SupabaseClient) InsertTelemetry(telem Telemetry) error {
}

func (s SupabaseClient) UpdateTelemetry(telem Telemetry) error {
}
