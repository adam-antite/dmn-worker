package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"log"
	"os"
	"time"
)

func getMembershipData(httpClient *resty.Client, bungieMembershipId string) (map[string]interface{}, time.Duration, error) {
	apiKey := os.Getenv("BUNGIE_API_KEY")
	var result map[string]interface{}

	bungieLimiter.Take()
	membershipTime := time.Now()
	resp, err := httpClient.R().
		SetHeader("X-API-Key", apiKey).
		Get(fmt.Sprintf("https://www.bungie.net/Platform/User/GetMembershipsById/%s/-1", bungieMembershipId))
	if err != nil {
		log.Println("Error getting membership data: ", err)
		return nil, time.Since(membershipTime), err
	}
	err = json.Unmarshal(resp.Body(), &result)
	if err != nil {
		log.Println("Error unmarshalling membership data: ", err)
		return nil, time.Since(membershipTime), err
	}
	return result, time.Since(membershipTime), nil
}

func getProfile(httpClient *resty.Client, destinyMembershipId string, membershipType int) (map[string]interface{}, time.Duration, error) {
	var result map[string]interface{}
	apiKey := os.Getenv("BUNGIE_API_KEY")

	bungieLimiter.Take()
	profileTime := time.Now()
	resp, err := httpClient.R().
		SetHeader("X-API-Key", apiKey).
		SetQueryString("components=100,800").
		Get(fmt.Sprintf("https://www.bungie.net/Platform/Destiny2/%d/Profile/%s/", membershipType, destinyMembershipId))

	if resp.StatusCode() == 503 {
		log.Println("Bungie API is unavailable, status code 503")
		log.Println(resp)
		panic(err)
	}
	if err != nil {
		log.Println("Error getting profile: ", err)
		return nil, time.Since(profileTime), err
	}
	err = json.Unmarshal(resp.Body(), &result)
	if err != nil {
		log.Println("Error unmarshalling profile: ", err)
		return nil, time.Since(profileTime), err
	}

	//file, _ := json.MarshalIndent(result, "", " ")
	//_ = os.WriteFile("profile.json", file, 0644)

	return result, time.Since(profileTime), nil
}
