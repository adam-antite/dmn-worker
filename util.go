package main

import (
	"log"
	"os"
)

func createFile(dirName string, fileName string) (*os.File, error) {
	currentDir, _ := os.Getwd()
	dirPath := currentDir + "/" + dirName + "/"
	filePath := dirPath + fileName

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		err := os.MkdirAll(dirPath, 0700)
		if err != nil {
			log.Printf("error creating directory %s: %s\n", dirName, err.Error())
			return nil, err
		}
	}

	file, err := os.Create(filePath)
	if err != nil {
		log.Printf("error creating %s: %s\n", fileName, err.Error())
		return nil, err
	}

	return file, nil
}
