package main

import (
	"flag"
	"fmt"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
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

var (
	jobId                string
	messageCount         int
	userCount            int
	currentTime          string
	isRunningInContainer *bool

	wg sync.WaitGroup

	//discord *discordgo.Session
	//storageManager *S3Manager
	//supabase *supa.Client

	usersChannel chan User
)

type App struct {
	storageManager   StorageManager
	databaseClient   DatabaseClient
	bungieApiLimiter ratelimit.Limiter
	consumerCount    int64
	discord          *discordgo.Session
}

func NewApp(storageManager StorageManager, databaseClient DatabaseClient, limiter ratelimit.Limiter, consumerCount int64, discordSession *discordgo.Session) *App {
	return &App{
		storageManager:   storageManager,
		databaseClient:   databaseClient,
		bungieApiLimiter: limiter,
		consumerCount:    consumerCount,
		discord:          discordSession,
	}
}

func init() {
	messageCount = 0
	jobId = uuid.New().String()
	log.Println("starting worker with id:", jobId)

	isRunningInContainer = flag.Bool("container", false, "running inside container: true or false")
	flag.Parse()

	if !*isRunningInContainer {
		err := godotenv.Load()
		if err != nil {
			log.Fatal("error loading .env file")
		}
	}

	currentTime = time.Now().Format(time.RFC3339)
}

func main() {
	defer track()()

	consumerCount, _ := strconv.ParseInt(os.Getenv("WORKER_COUNT"), 10, 64)

	databaseClient := NewSupabaseClient(os.Getenv("SUPABASE_URL"), os.Getenv("SUPABASE_SERVICE_ROLE_KEY"))

	storageManager, err := NewS3Manager()
	if err != nil {
		log.Println("error creating storage manager: ", err)
	}

	discordBotToken := os.Getenv("DISCORD_BOT_TOKEN")
	discord, err := discordgo.New("Bot " + discordBotToken)
	if err != nil {
		log.Println("error initializing discord bot: " + err.Error())
	}

	limiter := ratelimit.New(25)

	app := NewApp(storageManager, databaseClient, limiter, consumerCount, discord)
	app.Run()
}

func (a *App) Run() {
	wg.Add(1)
	go scan()
	wg.Wait()

	for i := 1; i <= int(a.consumerCount); i++ {
		wg.Add(1)
		go consume(usersChannel)
	}

	close(usersChannel)
	wg.Wait()
}

func scan() {
	defer wg.Done()

	var users []User
	usersChannel = make(chan User, userCount)

	for _, user := range users {
		usersChannel <- user
	}
}

func consume(users <-chan User) {
	defer wg.Done()
	for user := range users {
		a.limiter.Take()
		err := ProcessUser(user)
		if err != nil {
			log.Println("error processing user: ", err)
			panic(err)
		}
	}
}

func track() func() {
	var logFilePath string

	start := time.Now()
	err := os.MkdirAll(filepath.Join(".", "logs"), os.ModePerm)
	if err != nil {
		return nil
	}

	var results []map[string]interface{}
	telem := Telemetry{
		JobId:       jobId,
		StartTime:   &start,
		WorkerCount: consumerCount,
	}
	err = supabase.DB.From("telemetry").Insert(telem).Execute(&results)
	if err != nil {
		log.Println("error creating job telemetry record: ", err)
	} else {
		log.Println("successfully created job telemetry record")
		//log.Println(results)
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
			logFileName := logFile.Name()
			err := logFile.Close()
			if err != nil {
				log.Printf("error closing log file: " + err.Error())
			}

			if *isRunningInContainer {
				storageManager.UploadLogs(logFileName)
			}
		}(logFile)

		executionTime := time.Since(start).Truncate(time.Millisecond)
		consumptionRate := executionTime.Seconds() / float64(messageCount)
		processingRate := time.Duration(consumptionRate * float64(time.Second)).Truncate(time.Millisecond)
		log.Printf("\n========\n"+
			"Execution time: %s\n"+
			"Total users: %d\n"+
			"Processed users: %d\n"+
			"Processing rate: %s per user\n"+
			"========\n", executionTime, userCount, messageCount, processingRate)

		telem = Telemetry{
			TotalUsers:     int64(userCount),
			ProcessedUsers: int64(messageCount),
			ProcessingRate: processingRate.Seconds(),
			ExecutionTime:  executionTime.Seconds(),
		}
		err =
		err = supabase.DB.From("telemetry").Update(telem).Eq("id", jobId).Execute(&results)
		if err != nil {
			log.Println("error updating job telemetry: ", err)
		} else {
			log.Println("successfully uploaded job telemetry to db")
		}
	}
}
