package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
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

var (
	jobId                string
	bungieLimiter        ratelimit.Limiter
	messageCount         int
	userCount            int
	currentTime          string
	consumerWorkerCount  int64
	isRunningInContainer *bool
	err                  error

	wg sync.WaitGroup

	discord        *discordgo.Session
	storageManager *S3Manager
	supabase       *supa.Client

	usersChannel chan User
)

func init() {
	messageCount = 0
	jobId = uuid.New().String()
	bungieLimiter = ratelimit.New(25)
	log.Println("starting worker with id:", jobId)

	isRunningInContainer = flag.Bool("container", false, "running inside container: true or false")
	flag.Parse()

	if !*isRunningInContainer {
		err := godotenv.Load()
		if err != nil {
			log.Fatal("error loading .env file")
		}
	}

	consumerWorkerCount, _ = strconv.ParseInt(os.Getenv("WORKER_COUNT"), 10, 64)

	supabase = supa.CreateClient(os.Getenv("SUPABASE_URL"), os.Getenv("SUPABASE_SERVICE_ROLE_KEY"))

	storageManager, err = NewS3Manager()
	if err != nil {
		log.Println("error creating storage manager: ", err)
	}

	discordBotToken := os.Getenv("DISCORD_BOT_TOKEN")
	discord, err = discordgo.New("Bot " + discordBotToken)
	if err != nil {
		log.Println("error initializing discord bot: " + err.Error())
	}

	currentTime = time.Now().Format(time.RFC3339)
}

func main() {
	defer track()()

	wg.Add(1)
	go scan()
	wg.Wait()

	for i := 1; i <= int(consumerWorkerCount); i++ {
		wg.Add(1)
		go consume(usersChannel)
	}

	close(usersChannel)
	wg.Wait()
}

func scan() {
	defer wg.Done()
	start := time.Now()

	var results []map[string]interface{}

	log.Println("scanning users table...")

	err := supabase.DB.From("users").Select("*").Execute(&results)
	if err != nil {
		log.Println("error querying users table: ", err)
		panic(err)
	}

	userCount = len(results)
	usersChannel = make(chan User, userCount)

	for _, data := range results {
		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Println("error marshalling user data into json")
			panic(err)
		}

		user := User{}
		err = json.Unmarshal(jsonData, &user)
		if err != nil {
			log.Println("error unmarshalling user data into user struct")
		}

		usersChannel <- user
	}

	executionTime := time.Since(start)
	log.Printf("finished scanning after %s", executionTime.Truncate(time.Millisecond))
	log.Printf("user count: %d", userCount)
}

func consume(users <-chan User) {
	defer wg.Done()
	for user := range users {
		bungieLimiter.Take()
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
		WorkerCount: consumerWorkerCount,
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
		err = supabase.DB.From("telemetry").Update(telem).Eq("id", jobId).Execute(&results)
		if err != nil {
			log.Println("error updating job telemetry: ", err)
		} else {
			log.Println("successfully uploaded job telemetry to db")
		}
	}
}
