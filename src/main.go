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
	wg                   sync.WaitGroup
)

type App struct {
	storageManager StorageManager
	databaseClient DatabaseClient
	bungieClient   BungieClient
	consumerCount  int64
	discord        *discordgo.Session
	usersChannel   chan User
}

func NewApp(storageManager StorageManager, databaseClient DatabaseClient, bungieClient BungieClient, consumerCount int64, discordSession *discordgo.Session) *App {
	return &App{
		storageManager: storageManager,
		databaseClient: databaseClient,
		bungieClient:   bungieClient,
		consumerCount:  consumerCount,
		discord:        discordSession,
		usersChannel:   make(chan User),
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
	storageManager, err := NewS3Manager()
	if err != nil {
		log.Println("error creating storage manager: ", err)
	}

	databaseClient := NewSupabaseClient(os.Getenv("SUPABASE_URL"), os.Getenv("SUPABASE_SERVICE_ROLE_KEY"))
	bungieClient := NewBungieClient(ratelimit.New(25))
	consumerCount, _ := strconv.ParseInt(os.Getenv("WORKER_COUNT"), 10, 64)

	discordBotToken := os.Getenv("DISCORD_BOT_TOKEN")
	discord, err := discordgo.New("Bot " + discordBotToken)
	if err != nil {
		log.Println("error initializing discord bot: " + err.Error())
	}

	app := NewApp(storageManager, databaseClient, *bungieClient, consumerCount, discord)
	app.Run()
}

func (a *App) Run() {
	defer a.track()()

	users := a.scan()

	for i := 1; i <= int(a.consumerCount); i++ {
		wg.Add(1)
		go a.consume(a.usersChannel)
	}

	for _, user := range users {
		a.usersChannel <- user
	}

	close(a.usersChannel)
	wg.Wait()
}

func (a *App) scan() []User {
	return a.databaseClient.GetAllUsers()
}

func (a *App) consume(users <-chan User) {
	defer wg.Done()
	for user := range users {
		//a.bungieClient.apiLimiter.Take()
		err := a.ProcessUser(user)
		if err != nil {
			log.Println("error processing user: ", err)
			panic(err)
		}
	}
}

func (a *App) track() func() {
	var logFilePath string

	start := time.Now()
	err := os.MkdirAll(filepath.Join(".", "logs"), os.ModePerm)
	if err != nil {
		return nil
	}

	telem := Telemetry{
		JobId:       jobId,
		StartTime:   &start,
		WorkerCount: a.consumerCount,
	}
	a.databaseClient.UpdateTelemetry(telem)

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
				a.storageManager.UploadWorkerLogs(logFileName)
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
		a.databaseClient.UpdateTelemetry(telem)
	}
}
