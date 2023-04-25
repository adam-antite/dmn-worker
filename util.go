package main

import (
	"log"
	"os"
	"strconv"
	"time"
)

func CreateFile(dirName string, fileName string) (*os.File, error) {
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

func GetPreviousTuesday() string {
	var tuesdayDistance int
	var tuesdayIndex = 2

	today := time.Now()
	todayIndex := int(today.Weekday())
	todayCurrentTime := today.UTC().Format("1504")
	todayCurrentTimeInt, _ := strconv.ParseInt(todayCurrentTime, 10, 32)

	tuesdayDelta := todayIndex - tuesdayIndex
	if todayCurrentTimeInt < 1700 && todayIndex == tuesdayIndex { // if its tuesday but before destiny reset
		tuesdayDistance = 7
	} else if tuesdayDelta < 0 {
		tuesdayDistance = 7 + tuesdayDelta
	} else {
		tuesdayDistance = tuesdayDelta
	}
	previousTuesday := today.AddDate(0, 0, -tuesdayDistance).Format("2006-01-02")

	log.Printf("Current time (UTC): %s", todayCurrentTime)
	log.Printf("Previous Tuesday: %s", previousTuesday)

	return previousTuesday
}
