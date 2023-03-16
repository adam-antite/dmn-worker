package main

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	"log"
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

	// Determine directMessageContent content and send
	directMessageContent := buildDirectMessageContent(missingShaders)
	dmChannel, err := discord.UserChannelCreate(user.UserId)
	if err != nil {
		log.Printf("(Request ID: %s) Error creating DM channel for user with ID %s: %s\n", requestId, user.UserId, err.Error())
	}

	_, err = discord.ChannelMessageSend(dmChannel.ID, directMessageContent)
	if err != nil {
		log.Printf("(Request ID: %s) Error sending direct message to user with ID %s: %s\n", requestId, user.UserId, err.Error())
	} else {
		log.Printf("(Request ID: %s) Successfully sent message")
	}

	log.Printf("(Request ID: %s) Finished in %s\n", requestId, time.Since(start))
	messageCount++
	return nil
}

func getMissingCollectibles(profile map[string]interface{}) []string {
	var missingCollectibles []string
	var notAcquired = 1

	for collectible, state := range profile["Response"].(map[string]interface{})["profileCollectibles"].(map[string]interface{})["data"].(map[string]interface{})["collectibles"].(map[string]interface{}) {
		if _, isShader := masterShadersList[collectible]; isShader {
			stateValue := int(state.(map[string]interface{})["state"].(float64))
			if stateValue&notAcquired == 1 {
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

func buildDirectMessageContent(missingShaders []string) string {
	var message string

	// Missing shaders available from Ada
	if len(missingShaders) > 0 {
		message = fmt.Sprintf(
			"Ada-1 is selling shaders you don't have: %s!",
			strings.Join(missingShaders, ", "))
	}

	// No missing shaders available from Ada
	if len(missingShaders) == 0 {
		message = "There are no new shaders available for you this week."
	}

	return message
}
