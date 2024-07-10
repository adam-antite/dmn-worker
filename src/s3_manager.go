package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"log"
	"os"
	"path/filepath"
)

type StorageManager interface {
	UploadWorkerLogs(logFileName string)
	DownloadVendorShaders() map[string]interface{}
	DownloadMasterShaderList() map[string]interface{}
	GetVendorShaders() map[string]interface{}
	GetMasterShaderList() map[string]interface{}
}

type S3Manager struct {
	client            *s3.Client
	downloader        *manager.Downloader
	uploader          *manager.Uploader
	vendorShaders     map[string]interface{}
	masterShadersList map[string]interface{}
}

func NewS3Manager() (*S3Manager, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Println("error loading s3 config: ", err)
		return nil, err
	}

	client := s3.NewFromConfig(cfg)

	s := &S3Manager{
		client:     client,
		downloader: manager.NewDownloader(client),
		uploader:   manager.NewUploader(client),
	}

	s.vendorShaders = s.DownloadVendorShaders()
	s.masterShadersList = s.DownloadMasterShaderList()

	return s, nil
}

func (s *S3Manager) DownloadVendorShaders() map[string]interface{} {
	file, _ := CreateFile("json-data", "vendor-shaders.json")
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("error closing vendor shaders list")
		}
	}(file)

	previousTuesday := GetPreviousTuesday()

	// Download vendor shader list
	numBytes, err := s.downloader.Download(context.TODO(), file,
		&s3.GetObjectInput{
			Bucket: aws.String("dmn-storage"),
			Key:    aws.String(fmt.Sprintf("vendor-shaders/%s.json", previousTuesday)),
		})
	if err != nil {
		log.Println("error retrieving vendor shaders list from S3.")
		log.Fatal(err)
	}
	log.Println("downloaded", file.Name(), numBytes, "bytes")

	byteValues, _ := io.ReadAll(file)
	var result map[string]interface{}
	err = json.Unmarshal(byteValues, &result)
	if err != nil {
		log.Println("error converting vendor shaders download to map")
		return nil
	}

	return result
}

func (s *S3Manager) DownloadMasterShaderList() map[string]interface{} {
	file, _ := CreateFile("json-data", "master-shader-collectible-list.json")
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("error closing master shader collectible list")
		}
	}(file)

	// Download master shader list (collectible hash as key)
	numBytes, err := s.downloader.Download(context.TODO(), file,
		&s3.GetObjectInput{
			Bucket: aws.String("dmn-storage"),
			Key:    aws.String("master-shader-collectible-list.json"),
		})
	if err != nil {
		log.Println("error retrieving master shader list from S3.")
		log.Fatal(err)
	}
	log.Println("downloaded", file.Name(), numBytes, "bytes")

	byteValues, _ := io.ReadAll(file)
	var result map[string]interface{}
	err = json.Unmarshal(byteValues, &result)
	if err != nil {
		log.Println("error converting master shader list download to map")
		return nil
	}

	return result
}

func (s *S3Manager) GetVendorShaders() map[string]interface{} {
	return s.vendorShaders
}

func (s *S3Manager) GetMasterShaderList() map[string]interface{} {
	return s.masterShadersList
}

func (s *S3Manager) UploadWorkerLogs(filePath string) {
	file, err := os.Open(filePath)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Printf("error closing log file: " + err.Error())
		}
	}(file)

	_, fileName := filepath.Split(file.Name())

	_, err = s.uploader.Upload(context.TODO(), &s3.PutObjectInput{
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
