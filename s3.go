package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"log"
	"os"
	"path/filepath"
)

func getVendorShaders() map[string]interface{} {
	file, _ := createFile("json-data", "vendor-shaders.json")
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("error closing vendor shaders list")
		}
	}(file)

	previousTuesday := getPreviousTuesday()

	// Download vendor shader list
	numBytes, err := s3downloader.Download(context.TODO(), file,
		&s3.GetObjectInput{
			Bucket: aws.String("dmn-storage"),
			Key:    aws.String(fmt.Sprintf("vendor-shaders/%s.json", previousTuesday)),
		})
	if err != nil {
		log.Println("Error retrieving vendor shaders list from S3.")
		log.Fatal(err)
	}
	log.Println("Downloaded", file.Name(), numBytes, "bytes")

	byteValues, _ := io.ReadAll(file)
	var result map[string]interface{}
	err = json.Unmarshal(byteValues, &result)
	if err != nil {
		log.Println("Error converting vendor shaders download to map")
		return nil
	}

	return result
}

func getMasterShaderList() map[string]interface{} {
	file, _ := createFile("json-data", "master-shader-collectible-list.json")
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("error closing master shader collectible list")
		}
	}(file)

	// Download master shader list (collectible hash as key)
	numBytes, err := s3downloader.Download(context.TODO(), file,
		&s3.GetObjectInput{
			Bucket: aws.String("dmn-storage"),
			Key:    aws.String("master-shader-collectible-list.json"),
		})
	if err != nil {
		log.Println("error retrieving master shader list from S3.")
		log.Fatal(err)
	}
	log.Println("Downloaded", file.Name(), numBytes, "bytes")

	byteValues, _ := io.ReadAll(file)
	var result map[string]interface{}
	err = json.Unmarshal(byteValues, &result)
	if err != nil {
		log.Println("error converting master shader list download to map")
		return nil
	}

	return result
}

func uploadLogs(file *os.File) {
	_, fileName := filepath.Split(file.Name())

	_, err := s3uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("dmn-storage"),
		Key:    aws.String("logs/" + fileName),
		Body:   file,
	})
	if err != nil {
		log.Printf("error uploading file to s3: %s", err)
		return
	}
	log.Println("log file successfully uploaded to s3")
}
