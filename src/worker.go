package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func (a *App) ProcessUser(user User) error {
	start := time.Now()
	requestId := uuid.New().String()
	httpClient := resty.New()
	httpClient.
		SetRetryCount(3).
		SetRetryWaitTime(5 * time.Second).
		SetRetryMaxWaitTime(20 * time.Second)

	if user.BungieMembershipId == 0 {
		log.Printf("(Request ID: %s) User has not linked Bungie account, skipping user: %d\n", requestId, int64(user.DiscordId))
		return nil
	}

	membershipData, membershipDataTime, err := a.bungieClient.GetDestinyMembershipData(httpClient, strconv.FormatFloat(user.BungieMembershipId, 'f', -1, 64))
	if err != nil {
		log.Printf("(Request ID: %s) Error getting Bungie membership data: %s\n", requestId, err)
		return err
	}
	log.Printf("(Request ID: %s) Getting Bungie membership data took %s\n", requestId, membershipDataTime.Truncate(time.Millisecond))

	membershipType := gjson.Get(membershipData, "Response.destinyMemberships.0.membershipType").Int()
	destinyMembershipId := gjson.Get(membershipData, "Response.destinyMemberships.0.membershipId").String()

	profile, profileTime, err := a.bungieClient.GetDestinyProfile(httpClient, destinyMembershipId, membershipType)
	if err != nil {
		log.Printf("(Request ID: %s) Error getting profile: %s\n", requestId, err)
		return err
	}
	log.Printf("(Request ID: %s) Getting Bungie profile took %s\n", requestId, profileTime.Truncate(time.Millisecond))

	missingShadersTime := time.Now()
	missingCollectibleShaders := a.getMissingCollectibleShaders(profile)
	missingAdaShaders := a.getMissingAdaShaders(missingCollectibleShaders)

	log.Printf("(Request ID: %s) Checking missing shaders took %s\n", requestId, time.Since(missingShadersTime).Truncate(time.Millisecond))

	if len(missingAdaShaders) == 0 {
		log.Printf("(Request ID: %s) User has no missing shaders available from Ada-1\n", requestId)
	} else {
		log.Printf("(Request ID: %s) User has missing shaders available from Ada-1: %v\n", requestId, strings.Join(missingAdaShaders, ", "))
	}

	if os.Getenv("SEND_MESSAGES") == "true" {
		directMessageContent := buildDirectMessageContent(missingAdaShaders)

		if directMessageContent != "" {
			dmChannel, err := a.discord.UserChannelCreate(strconv.FormatInt(int64(user.DiscordId), 10))
			if err != nil {
				log.Printf("(Request ID: %s) Error creating DM channel for user %d: %s\n", requestId, int64(user.DiscordId), err.Error())
			}

			_, err = a.discord.ChannelMessageSend(dmChannel.ID, directMessageContent)
			if err != nil {
				log.Printf("(Request ID: %s) Error sending direct message to user %d: %s\n", requestId, int64(user.DiscordId), err.Error())
			} else {
				log.Printf("(Request ID: %s) Successfully sent message to user %d", requestId, int64(user.DiscordId))
			}
		} else {
			log.Printf("(Request ID: %s) Skipped sending message to user %d\n", requestId, int64(user.DiscordId))
		}
	} else {
		log.Printf("(Request ID: %s) Messaging disabled, skipped sending message to user %d\n", requestId, int64(user.DiscordId))
	}

	log.Printf("(Request ID: %s) Finished in %s\n", requestId, time.Since(start).Truncate(time.Millisecond))
	messageCount++
	return nil
}

func (a *App) getMissingCollectibleShaders(profile string) []string {
	var missingCollectibleShaders []string
	var collectibles map[string]interface{}
	var notAcquired = 1

	err := json.Unmarshal([]byte(gjson.Get(profile, "Response.profileCollectibles.data.collectibles").String()), &collectibles)
	if err != nil {
		log.Println("Error unmarshalling collectibles from profile: ", err)
	}

	for collectibleHash, state := range collectibles {
		if _, isShader := a.storageManager.GetMasterShaderList()[collectibleHash]; isShader {
			stateValue := int(state.(map[string]interface{})["state"].(float64))
			if stateValue&notAcquired == 1 {
				missingCollectibleShaders = append(missingCollectibleShaders, a.storageManager.GetMasterShaderList()[collectibleHash].(map[string]interface{})["hash"].(string))
			}
		}
	}

	return missingCollectibleShaders
}

func (a *App) getMissingAdaShaders(missingCollectibleShaders []string) []string {
	var missingAdaShaders []string

	for shaderHash, shaderInfo := range a.storageManager.GetVendorShaders() {
		for _, missingCollectible := range missingCollectibleShaders {
			if missingCollectible == shaderHash {
				missingAdaShaders = append(missingAdaShaders, shaderInfo.(map[string]interface{})["name"].(string))
				break
			}
		}
	}

	return missingAdaShaders
}

func buildDirectMessageContent(missingShaders []string) string {
	var message = ""

	if len(missingShaders) > 0 {
		message = fmt.Sprintf(
			"Ada-1 is selling shaders you don't have: %s!",
			strings.Join(missingShaders, ", "))
	} else {
		message = fmt.Sprintf("Ada-1 is not selling any new shaders for you this week.")
	}

	return message
}
