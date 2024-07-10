package main

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"go.uber.org/ratelimit"
	"log"
	"os"
	"time"
)

type BungieClient struct {
	apiLimiter ratelimit.Limiter
}

func NewBungieClient(rateLimiter ratelimit.Limiter) *BungieClient {
	return &BungieClient{
		apiLimiter: rateLimiter,
	}
}

func (c BungieClient) GetDestinyMembershipData(httpClient *resty.Client, bungieMembershipId string) (string, time.Duration, error) {
	var result string
	apiKey := os.Getenv("BUNGIE_API_KEY")

	c.apiLimiter.Take()
	membershipTime := time.Now()
	resp, err := httpClient.R().
		SetHeader("X-API-Key", apiKey).
		Get(fmt.Sprintf("https://www.bungie.net/Platform/User/GetMembershipsById/%s/-1", bungieMembershipId))
	if err != nil {
		log.Println("Error getting membership data: ", err)
		return "", time.Since(membershipTime), err
	}
	result = string(resp.Body())

	return result, time.Since(membershipTime), nil
}

func (c BungieClient) GetDestinyProfile(httpClient *resty.Client, destinyMembershipId string, membershipType int64) (string, time.Duration, error) {
	var result string
	apiKey := os.Getenv("BUNGIE_API_KEY")

	c.apiLimiter.Take()
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
		return "", time.Since(profileTime), err
	}
	result = string(resp.Body())
	log.Println("Profile size:", len(resp.Body())/1024, "KB")

	//file, _ := json.MarshalIndent(result, "", " ")
	//_ = os.WriteFile("profile.json", file, 0644)

	return result, time.Since(profileTime), nil
}
