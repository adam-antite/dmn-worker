package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bwmarrin/discordgo"
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
var consumerWorkerCount int32
var scanWorkerCount int32
var wg sync.WaitGroup
var scanWg sync.WaitGroup

var discord *discordgo.Session
var s3downloader *manager.Downloader

var usersChannel chan User

var vendorShadersMap map[string]interface{}
var masterShadersList map[string]interface{}

var capacityUnitsTotal = 0.0

type User struct {
	UserId             string   `dynamodbav:"userId"`
	BungieMembershipId string   `dynamodbav:"bungieMembershipId"`
	FcmTokens          []string `dynamodbav:"fcmTokens"`
}

func init() {
	messageCount = 0
	consumerWorkerCount = 25
	scanWorkerCount = 1
	bungieLimiter = ratelimit.New(25)

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	s3client := s3.NewFromConfig(cfg)
	s3downloader = manager.NewDownloader(s3client)

	discordBotToken := os.Getenv("DISCORD_BOT_TOKEN")
	discord, err = discordgo.New("Bot " + discordBotToken)
	if err != nil {
		log.Println("Error initializing discord bot: " + err.Error())
	}

	vendorShadersMap = getVendorShaders()
	masterShadersList = getMasterShaderList()

	currentTime = time.Now().Format(time.RFC3339)
}

func main() {
	defer track("main")()

	usersChannel = make(chan User)

	for i := 0; i < int(scanWorkerCount); i++ {
		scanWg.Add(1)
		go scan(int32(i), usersChannel)
	}

	for i := 1; i <= int(consumerWorkerCount); i++ {
		wg.Add(1)
		go consume(usersChannel)
	}

	scanWg.Wait()
	close(usersChannel)

	wg.Wait()
}

func scan(segment int32, usersChannel chan<- User) {
	defer scanWg.Done()
	var scannedUsers []User

	options := dynamodb.Options{
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY"),
			os.Getenv("AWS_SECRET_KEY"),
			"")),
		Region: os.Getenv("AWS_REGION"),
	}
	svc := dynamodb.New(options)
	paginator := dynamodb.NewScanPaginator(svc, &dynamodb.ScanInput{
		TableName:              aws.String("users"),
		ReturnConsumedCapacity: "TOTAL",
		Segment:                &segment,
		TotalSegments:          &scanWorkerCount,
	})

	for paginator.HasMorePages() {
		out, err := paginator.NextPage(context.TODO())
		capacityUnitsTotal += *out.ConsumedCapacity.CapacityUnits
		if err != nil {
			panic(err)
		}

		err = attributevalue.UnmarshalListOfMaps(out.Items, &scannedUsers)
		if err != nil {
			panic(err)
		}

		for _, user := range scannedUsers {
			usersChannel <- user
		}
	}
}

func consume(users <-chan User) {
	defer wg.Done()
	for user := range users {
		bungieLimiter.Take()
		err := checkUserShaders(user)
		if err != nil {
			panic(err)
		}
	}
}

func track(name string) func() {
	start := time.Now()
	err := os.MkdirAll(filepath.Join(".", "logs"), os.ModePerm)
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
		log.Printf("%s\n========\nExecution time: %s\nProcessed users: %d\nProcessing rate: %.2f users per second\nConsumed read capacity units: %.2f\n", name, executionTime.Truncate(time.Second), messageCount, consumptionRate, capacityUnitsTotal)
	}
}
