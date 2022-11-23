package main

import (
	"context"
	firebase "firebase.google.com/go"
	"firebase.google.com/go/messaging"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
	"go.uber.org/ratelimit"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var bungieLimiter ratelimit.Limiter
var messageCount int
var currentTime string
var consumerCount int
var wg sync.WaitGroup

var firebaseApp *firebase.App
var fcmClient *messaging.Client
var s3downloader *manager.Downloader

var vendorModsMap map[string]interface{}
var masterModsMap map[string]interface{}

var capacityUnitsTotal = 0.0

type User struct {
	UserId             string   `dynamodbav:"userId"`
	BungieMembershipId string   `dynamodbav:"bungieMembershipId"`
	FcmTokens          []string `dynamodbav:"fcmTokens"`
}

func init() {
	consumerCount = 15
	bungieLimiter = ratelimit.New(25)

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	messageCount = 0
	currentTime = time.Now().Format(time.RFC3339)

	cfg, err := config.LoadDefaultConfig(context.TODO())
	s3client := s3.NewFromConfig(cfg)
	s3downloader = manager.NewDownloader(s3client)

	firebaseApp, err = firebase.NewApp(context.Background(), nil)
	if err != nil {
		log.Fatalf("error initializing firebase app: %v\n", err)
	}

	ctx := context.Background()
	fcmClient, err = firebaseApp.Messaging(ctx)
	if err != nil {
		log.Fatalf("Error getting Messaging client: %v\n", err)
	}

	currentTime = time.Now().Format(time.RFC3339)

	vendorModsMap = getVendorMods()
	masterModsMap = getMasterModList()
}

func main() {
	defer track("main")()

	users := make(chan User)

	go scan(users)

	for i := 1; i <= consumerCount; i++ {
		wg.Add(1)
		go consume(users)
	}

	wg.Wait()
}

func scan(usersChannel chan<- User) {
	defer close(usersChannel)
	var users []User

	options := dynamodb.Options{
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY"), os.Getenv("AWS_SECRET_KEY"), "")),
		Region:      os.Getenv("AWS_REGION"),
	}
	svc := dynamodb.New(options)
	paginator := dynamodb.NewScanPaginator(svc, &dynamodb.ScanInput{
		TableName:              aws.String("users"),
		ReturnConsumedCapacity: "TOTAL",
	})

	for paginator.HasMorePages() {
		out, err := paginator.NextPage(context.TODO())
		capacityUnitsTotal += *out.ConsumedCapacity.CapacityUnits
		if err != nil {
			panic(err)
		}

		err = attributevalue.UnmarshalListOfMaps(out.Items, &users)
		if err != nil {
			panic(err)
		}

		for _, user := range users {
			usersChannel <- user
		}
	}
}

func consume(users <-chan User) {
	defer wg.Done()
	for user := range users {
		bungieLimiter.Take()
		err := checkUserMods(user)
		if err != nil {
			fmt.Printf("Error in checkUserMods")
			panic(err)
		}
	}
}

func track(name string) func() {
	start := time.Now()
	newpath := filepath.Join(".", "logs")
	err := os.MkdirAll(newpath, os.ModePerm)
	if err != nil {
		return nil
	}
	return func() {
		LogFile := fmt.Sprintf("./logs/log_%s.log", currentTime)
		logFile, err := os.OpenFile(LogFile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Printf(err.Error())
		}
		defer func(logFile *os.File) {
			err := logFile.Close()
			if err != nil {
				log.Printf("Error closing log file: " + err.Error())
			}
		}(logFile)

		mw := io.MultiWriter(os.Stdout, logFile)
		log.SetOutput(mw)

		executionTime := time.Since(start)
		consumptionRate := float64(messageCount) / executionTime.Seconds()
		log.Printf("%s\n========\nExecution time: %s\nConsumed %d messages\nConsumption rate: %.2f messages per second\n", name, executionTime.Truncate(time.Second), messageCount, consumptionRate)
	}
}
