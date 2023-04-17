package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bwmarrin/discordgo"
	"github.com/joho/godotenv"
	supa "github.com/nedpals/supabase-go"
	"go.uber.org/ratelimit"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var bungieLimiter ratelimit.Limiter
var messageCount int
var currentTime string
var consumerWorkerCount int64

var wg sync.WaitGroup
var scanWg sync.WaitGroup

var discord *discordgo.Session
var s3downloader *manager.Downloader
var supabase *supa.Client

var usersChannel chan User

var vendorShadersMap map[string]interface{}
var masterShadersList map[string]interface{}

type User struct {
	DiscordId          float64 `json:"discord_id"`
	BungieMembershipId float64 `json:"bungie_membership_id"`
	Ada1               bool    `json:"ada_1"`
	CreatedAt          string  `json:"created_at"`
}

func init() {
	log.Println("starting worker...")

	isRunningInContainer := flag.Bool("container", false, "running inside container: true or false")
	flag.Parse()

	messageCount = 0
	bungieLimiter = ratelimit.New(25)

	if !*isRunningInContainer {
		err := godotenv.Load()
		if err != nil {
			log.Fatal("Error loading .env file")
		}
	}

	consumerWorkerCount, _ = strconv.ParseInt(os.Getenv("WORKER_COUNT"), 10, 64)

	cfg, err := config.LoadDefaultConfig(context.TODO())
	s3client := s3.NewFromConfig(cfg)
	s3downloader = manager.NewDownloader(s3client)

	supabase = supa.CreateClient(os.Getenv("SUPABASE_URL"), os.Getenv("SUPABASE_SERVICE_ROLE_KEY"))

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

	usersChannel = make(chan User, consumerWorkerCount)

	scanWg.Add(1)
	go scan(usersChannel)

	for i := 1; i <= int(consumerWorkerCount); i++ {
		wg.Add(1)
		go consume(usersChannel)
	}

	scanWg.Wait()
	close(usersChannel)

	wg.Wait()
}

func scan(usersChannel chan<- User) {
	defer scanWg.Done()
	var results []map[string]interface{}

	log.Println("scanning users table...")

	err := supabase.DB.From("users").Select("*").Execute(&results)
	if err != nil {
		panic(err)
	}

	for _, data := range results {
		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Println("error marshaling user data into json")
			panic(err)
		}

		user := User{}
		err = json.Unmarshal(jsonData, &user)
		if err != nil {
			log.Println("error unmarshaling user data into user struct")
		}

		usersChannel <- user
	}

	log.Println("finished scanning.")
	log.Printf("user count: %d", len(results))
}

func consume(users <-chan User) {
	defer wg.Done()
	for user := range users {
		bungieLimiter.Take()
		err := processUser(user)
		if err != nil {
			panic(err)
		}
	}
}

func track(name string) func() {
	var logFilePath string

	start := time.Now()
	err := os.MkdirAll(filepath.Join(".", "logs"), os.ModePerm)
	if err != nil {
		return nil
	}

	//goland:noinspection GoBoolExpressions
	if runtime.GOOS == "windows" {
		logFilePath = fmt.Sprintf("./logs/log_%s.log", currentTime)
		logFilePath = strings.Replace(logFilePath, ":", ".", -1)
	} else {
		logFilePath = fmt.Sprintf("./logs/log_%s.log", currentTime)
	}
	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Printf(err.Error())
	}

	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	return func() {
		defer func(logFile *os.File) {
			err := logFile.Close()
			if err != nil {
				log.Printf("Error closing log file: " + err.Error())
			}
		}(logFile)

		executionTime := time.Since(start)
		consumptionRate := executionTime.Seconds() / float64(messageCount)
		log.Printf("%s\n========\nExecution time: %s\nProcessed users: %d\nProcessing rate: %.2f seconds per user\n", name, executionTime.Truncate(time.Millisecond), messageCount, consumptionRate)
	}
}
