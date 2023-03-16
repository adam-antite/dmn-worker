package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io/ioutil"
	"log"
	"os"
	"time"
)

func getVendorShaders() map[string]interface{} {
	var tuesdayDistance int
	var tuesdayIndex = 2

	file, err := os.Create("vendor-shaders.json")
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("error closing vendor shaders list")
		}
	}(file)

	// Determine date of previous Tuesday
	today := time.Now()
	todayIndex := int(today.Weekday())
	tuesdayDelta := todayIndex - tuesdayIndex
	if tuesdayDelta < 0 {
		tuesdayDistance = 7 - tuesdayDelta
	} else {
		tuesdayDistance = tuesdayDelta
	}
	previousTuesday := today.AddDate(0, 0, -tuesdayDistance).Format("2006-01-02")

	numBytes, err := s3downloader.Download(context.TODO(), file,
		&s3.GetObjectInput{
			Bucket: aws.String("dmn-storage"),
			Key:    aws.String(fmt.Sprintf("vendor-shaders/%s.json", previousTuesday)),
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
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("error closing master shader collectible list")
		}
	}(file)

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
