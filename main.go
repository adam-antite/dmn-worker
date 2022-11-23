package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/joho/godotenv"
	"go.uber.org/ratelimit"
	"log"
	"os"
	"sync"
)

var rl ratelimit.Limiter
var consumerCount int
var wg sync.WaitGroup
var capacityUnitsTotal = 0.0
var totalUsersConsumed = 0

type User struct {
	UserId             string   `dynamodbav:"userId"`
	BungieMembershipId string   `dynamodbav:"bungieMembershipId"`
	FcmTokens          []string `dynamodbav:"fcmTokens"`
}

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	consumerCount = 10
	rl = ratelimit.New(25)
}

func main() {
	users := make(chan User)

	go scan(users)

	for i := 1; i <= consumerCount; i++ {
		wg.Add(1)
		go consume(i, users)
	}

	wg.Wait()

	fmt.Printf("Total users consumed: %v", totalUsersConsumed)
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

func consume(worker int, users <-chan User) {
	defer wg.Done()
	for user := range users {
		rl.Take()
		fmt.Printf("User %+v is consumed by worker %v.\n", user, worker)
		totalUsersConsumed += 1
	}
}
