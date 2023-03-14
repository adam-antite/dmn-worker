package main

import (
	"context"
	"encoding/json"
	"firebase.google.com/go/messaging"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

func checkUserShaders(user User) error {
	start := time.Now()
	requestId := uuid.New().String()
	httpClient := resty.New()
	httpClient.
		SetRetryCount(3).
		SetRetryWaitTime(5 * time.Second).
		SetRetryMaxWaitTime(20 * time.Second)

	membershipData, err, membershipDataTime := getMembershipData(httpClient, user.BungieMembershipId)
	if err != nil {
		log.Printf("(Request ID: %s) Error getting Bungie membership data: %s\n", requestId, err)
		return err
	}
	log.Printf("(Request ID: %s) Getting Bungie membership data took %s\n", requestId, membershipDataTime)

	membershipType := int(membershipData["Response"].(map[string]interface{})["destinyMemberships"].([]interface{})[0].(map[string]interface{})["membershipType"].(float64))
	destinyMembershipId := membershipData["Response"].(map[string]interface{})["destinyMemberships"].([]interface{})[0].(map[string]interface{})["membershipId"].(string)

	profile, err, profileTime := getProfile(httpClient, destinyMembershipId, membershipType)
	if err != nil {
		log.Printf("(Request ID: %s) Error getting profile: %s\n", requestId, err)
		return err
	}
	log.Printf("(Request ID: %s) Getting Bungie profile took %s\n", requestId, profileTime)

	missingShadersTime := time.Now()
	missingCollectibles := getMissingCollectibles(profile)
	missingShaders := getMissingAdaShaders(missingCollectibles)

	log.Printf("(Request ID: %s) Checking missing shaders took %s\n", requestId, time.Since(missingShadersTime))

	if len(missingShaders) == 0 {
		log.Printf("(Request ID: %s) User has no missing shaders available from Ada-1\n", requestId)
	} else {
		log.Printf("(Request ID: %s) User has missing shaders available from Ada-1: %v\n", requestId, strings.Join(missingShaders, ", "))
	}

	// Determine notification content and send
	notification := getNotification(missingShaders)

	if len(user.FcmTokens) == 0 {
		log.Printf("(Request ID: %s) User has no FCM tokens, ignoring user", requestId)
	} else {
		message := &messaging.MulticastMessage{
			Notification: notification,
			Tokens:       user.FcmTokens,
		}
		ctx := context.Background()
		response, err := fcmClient.SendMulticast(ctx, message)
		if err != nil {
			log.Printf("(Request ID: %s) Error sending message: %s", requestId, err)
		} else {
			log.Printf("(Request ID: %s) Successfully sent messages: %+v\n", requestId, response)
		}

	}

	log.Printf("(Request ID: %s) Finished in %s\n", requestId, time.Since(start))
	messageCount++
	return nil
}

func getMembershipData(httpClient *resty.Client, bungieMembershipId string) (map[string]interface{}, error, time.Duration) {
	apiKey := os.Getenv("API_KEY")
	var result map[string]interface{}

	bungieLimiter.Take()
	membershipTime := time.Now()
	resp, err := httpClient.R().
		SetHeader("X-API-Key", apiKey).
		Get(fmt.Sprintf("https://www.bungie.net/Platform/User/GetMembershipsById/%s/-1", bungieMembershipId))
	if err != nil {
		log.Println("Error getting membership data: ", err)
		return nil, err, time.Since(membershipTime)
	}
	err = json.Unmarshal(resp.Body(), &result)
	if err != nil {
		log.Println("Error unmarshalling membership data: ", err)
		return nil, err, time.Since(membershipTime)
	}
	return result, nil, time.Since(membershipTime)
}

func getProfile(httpClient *resty.Client, destinyMembershipId string, membershipType int) (map[string]interface{}, error, time.Duration) {
	var result map[string]interface{}
	apiKey := os.Getenv("API_KEY")

	bungieLimiter.Take()
	profileTime := time.Now()
	resp, err := httpClient.R().
		SetHeader("X-API-Key", apiKey).
		SetQueryString("components=100,800").
		Get(fmt.Sprintf("https://www.bungie.net/Platform/Destiny2/%d/Profile/%s/", membershipType, destinyMembershipId))
	if err != nil {
		log.Println("Error getting profile: ", err)
		return nil, err, time.Since(profileTime)
	}
	err = json.Unmarshal(resp.Body(), &result)
	if err != nil {
		log.Println("Error unmarshalling profile: ", err)
		return nil, err, time.Since(profileTime)
	}
	return result, nil, time.Since(profileTime)
}

func getMissingCollectibles(profile map[string]interface{}) []string {
	var missingCollectibles []string
	var notAcquired = 1

	for collectible, state := range profile["Response"].(map[string]interface{})["profileCollectibles"].(map[string]interface{})["data"].(map[string]interface{})["collectibles"].(map[string]interface{}) {
		if _, isShader := masterShadersList[collectible]; isShader {
			stateValue := int(state.(map[string]interface{})["state"].(float64))
			if stateValue & notAcquired == 1 {
				//fmt.Printf("Shader name: %s, collectibleHash: %s, state: %d\n",
				//	shaderName,
				//	collectible,
				//	stateValue)
				missingCollectibles = append(missingCollectibles, masterShadersList[collectible].(map[string]interface{})["hash"].(string))
			}
		}
	}

	return missingCollectibles
}

func getMissingAdaShaders(missingCollectibles []string) []string {
	var missingAdaShaders []string

	for shaderHash, shaderInfo := range vendorShadersMap {
		for _, missingCollectible := range missingCollectibles {
			if missingCollectible == shaderHash {
				missingAdaShaders = append(missingAdaShaders, shaderInfo.(map[string]interface{})["name"].(string))
				break
			}
		}

	}

	return missingAdaShaders
}

func getNotification(missingShaders []string) *messaging.Notification {
	notification := &messaging.Notification{
		Title: "DestinyModsNotifier",
		Body:  "test",
	}

	// Missing shaders available from Ada
	if len(missingShaders) > 0 {
		notification.Body = fmt.Sprintf(
			"Ada-1 is selling shaders you don't have: %s!",
			strings.Join(missingShaders, ", "))
		return notification
	}

	// No missing shaders available from Ada
	if len(missingShaders) == 0 {
		notification.Body = "There are no new shaders available for you this week."
		return notification
	}

	return notification
}

func getVendorShaders() map[string]interface{} {
	file, err := os.Create("vendor-shaders.json")
	defer file.Close()

	today := time.Now().Format("2006-01-02")
	numBytes, err := s3downloader.Download(context.TODO(), file,
		&s3.GetObjectInput{
			Bucket: aws.String("dmn-storage"),
			Key:    aws.String(fmt.Sprintf("vendor-shaders/%s.json", today)),
		})
	if err != nil {
		log.Println("Error retrieving vendor shaders list from S3.")
		log.Fatal(err)
	}
	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")

	byteValues, _ := ioutil.ReadAll(file)
	var result map[string]interface{}
	err = json.Unmarshal(byteValues, &result)
	if err != nil {
		log.Println("Error converting vendor shaders download to map")
		return nil
	}

	return result
}

func getMasterShaderList() map[string]interface{} {
	file, err := os.Create("master-shader-collectible-list.json")
	defer file.Close()

	numBytes, err := s3downloader.Download(context.TODO(), file,
		&s3.GetObjectInput{
			Bucket: aws.String("dmn-storage"),
			Key:    aws.String("master-shader-collectible-list.json"),
		})
	if err != nil {
		log.Println("error retrieving master shader list from S3.")
		log.Fatal(err)
	}
	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")

	byteValues, _ := ioutil.ReadAll(file)
	var result map[string]interface{}
	err = json.Unmarshal(byteValues, &result)
	if err != nil {
		log.Println("error converting master shader list download to map")
		return nil
	}

	return result
}
