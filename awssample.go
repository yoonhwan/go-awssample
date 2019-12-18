package awssample

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func init() {
	v := "initilize aws sample"
	fmt.Printf("%q\n", v)
}

func StartSample() {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("ap-northeast-2"),
		// Credentials: credentials.NewStaticCredentials("", "", ""),
	},
	)
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("%+v\n", "connect complete aws.")

	DescribeDynamoDBTables(*sess)
	CreateDynamoDBTables(*sess)
	InsertDynamoDBTables(*sess)
	SelectDynamoDBTables(*sess)
	UpdateDynamoDBTables(*sess)
	DeleteDynamoDBTables(*sess)

	DescribeRegions(*sess)

	ch := make(chan int, 1)
	go testSQSSendMsg(*sess, testSQSInfo(*sess), ch)

	<-ch

	ch = make(chan int, 10)
	go testSQSReceiveMsg(*sess, testSQSInfo(*sess), ch)

	count := 0
OUT:
	for {
		time.Sleep(time.Second * 1)
		select {
		case data := <-ch:
			count += data
		default:
			count++
			fmt.Printf("wait.... %+v\n", count)
			if count > 100 {
				break OUT
			}
		}
	}
}

func DescribeRegions(sess session.Session) {
	svc := ec2.New(&sess)
	result, err := svc.DescribeRegions(&ec2.DescribeRegionsInput{})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
		return
	}
	fmt.Println(result)
	for k, v := range result.Regions {
		fmt.Println(k, v)
	}
}

func testSQSInfo(sess session.Session) string {
	// URL to our queue
	qURL := "QueueURL"

	// Create a SQS service client.
	svc := sqs.New(&sess)

	// List the queues available in a given region.
	result, err := svc.ListQueues(nil)
	if err != nil {
		fmt.Println("Error", err)
		return ""
	}

	fmt.Println("Success")

	// As these are pointers, printing them out directly would not be useful.
	for i, urls := range result.QueueUrls {
		// Avoid dereferencing a nil pointer.
		if urls == nil {
			continue
		}
		fmt.Printf("%d: %s\n", i, *urls)

		qURL = *urls
	}

	return qURL
}

func testSQSSendMsg(sess session.Session, qURL string, ch chan int) {

	for i := 0; i < 5; i++ {
		time.Sleep(time.Second * 2)

		// Create a SQS service client.
		svc := sqs.New(&sess)
		result, err := svc.SendMessage(&sqs.SendMessageInput{
			MessageGroupId: aws.String(fmt.Sprintf("test%d", i)),
			// DelaySeconds:   aws.Int64(10),
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				"Title": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String("The Whistler"),
				},
				"Author": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String("John Grisham"),
				},
				"WeeksOn": &sqs.MessageAttributeValue{
					DataType:    aws.String("Number"),
					StringValue: aws.String("6"),
				},
			},
			MessageBody: aws.String("Information about current NY Times fiction bestseller for week of " + time.Now().String()),
			QueueUrl:    &qURL,
		})

		if err != nil {
			fmt.Println("Error", err)
			return
		}

		fmt.Println("Success", result.String())
	}

	ch <- 1
}

func testSQSReceiveMsg(sess session.Session, qURL string, ch chan int) {

	// Create a SQS service client.
	svc := sqs.New(&sess)
	for {
		result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl: &qURL,
			AttributeNames: []*string{
				// aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
				aws.String(sqs.MessageSystemAttributeNameMessageGroupId),
			},
			MessageAttributeNames: []*string{
				// aws.String(sqs.QueueAttributeNameAll),
				aws.String(fmt.Sprintf("test%d", rand.Intn(4)+1)),
			},
			MaxNumberOfMessages: aws.Int64(1),
			VisibilityTimeout:   aws.Int64(20), // 20 seconds
			WaitTimeSeconds:     aws.Int64(5),
		})
		if err != nil {
			exitErrorf("Unable to receive message from queue %q, %v.", qURL, err)
		}
		if len(result.Messages) > 0 {
			fmt.Printf("Received %d messages.\n", len(result.Messages))

			go func() {
				for _, message := range result.Messages {
					// testSQSDeleteMsg(sess, qURL, svc, message)

					fmt.Println(message.String())
					ch <- 1
				}
			}()
		}
	}
}

func testSQSDeleteMsg(sess session.Session, qURL string, svc *sqs.SQS, message *sqs.Message) {
	resultDelete, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &qURL,
		ReceiptHandle: message.ReceiptHandle,
	})

	if err != nil {
		exitErrorf("Delete Error %q", err)
	}

	fmt.Println("Message Deleted", resultDelete.String())

}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func DescribeDynamoDBTables(sess session.Session) {
	svc := dynamodb.New(&sess)
	// create the input configuration instance
	input := &dynamodb.ListTablesInput{}

	fmt.Printf("Tables:\n")

	for {
		// Get the list of tables
		result, err := svc.ListTables(input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case dynamodb.ErrCodeInternalServerError:
					fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Println(err.Error())
			}
			return
		}

		for _, n := range result.TableNames {
			fmt.Println(*n)
		}

		// assign the last read tablename as the start for our next call to the ListTables function
		// the maximum number of table names returned in a call is 100 (default), which requires us to make
		// multiple calls to the ListTables function to retrieve all table names
		input.ExclusiveStartTableName = result.LastEvaluatedTableName

		if result.LastEvaluatedTableName == nil {
			break
		}
	}
}

func CreateDynamoDBTables(sess session.Session) {
	svc := dynamodb.New(&sess)
	// Create table Movies
	tableName := "Movies"

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Year"),
				AttributeType: aws.String("N"),
			},
			{
				AttributeName: aws.String("Title"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Year"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("Title"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String(tableName),
	}

	_, err := svc.CreateTable(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeResourceInUseException:
				fmt.Println(dynamodb.ErrCodeResourceInUseException, aerr.Error())
			case dynamodb.ErrCodeLimitExceededException:
				fmt.Println(dynamodb.ErrCodeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			exitErrorf("Create Error %q", err.Error())
		}
		return
	}

	fmt.Println("Created the table", tableName)
}

// Create struct to hold info about new item
type Item struct {
	Year   int
	Title  string
	Plot   string
	Rating float64
}

func InsertDynamoDBTables(sess session.Session) {
	svc := dynamodb.New(&sess)
	// Create table Movies
	item := Item{
		Year:   2015,
		Title:  "The Big New Movie",
		Plot:   "Nothing happens at all.",
		Rating: 0.0,
	}

	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		fmt.Println("Got error marshalling new movie item:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// Create item in table Movies
	movieName := "The Big New Movie"
	movieYear := 2015
	tableName := "Movies"

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}

	_, err = svc.PutItem(input)
	if err != nil {
		fmt.Println("Got error calling PutItem:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	year := strconv.Itoa(movieYear)

	fmt.Println("Successfully added '" + movieName + "' (" + year + ") to table " + tableName)

}

func SelectDynamoDBTables(sess session.Session) {
	svc := dynamodb.New(&sess)
	tableName := "Movies"
	movieName := "The Big New Movie"
	movieYear := "2015"

	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Year": {
				N: aws.String(movieYear),
			},
			"Title": {
				S: aws.String(movieName),
			},
		},
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	item := Item{}

	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	}

	if item.Title == "" {
		fmt.Println("Could not find '" + movieName + "' (" + movieYear + ")")
		return
	}

	fmt.Println("Found item:")
	fmt.Println("Year:  ", item.Year)
	fmt.Println("Title: ", item.Title)
	fmt.Println("Plot:  ", item.Plot)
	fmt.Println("Rating:", item.Rating)
}

func UpdateDynamoDBTables(sess session.Session) {
	svc := dynamodb.New(&sess)
	// Create item in table Movies
	tableName := "Movies"
	movieName := "The Big New Movie"
	movieYear := "2015"
	movieRating := "0.5"

	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":r": {
				N: aws.String(movieRating),
			},
		},
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Year": {
				N: aws.String(movieYear),
			},
			"Title": {
				S: aws.String(movieName),
			},
		},
		ReturnValues:     aws.String("UPDATED_NEW"),
		UpdateExpression: aws.String("set Rating = :r"),
	}

	_, err := svc.UpdateItem(input)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("Successfully updated '" + movieName + "' (" + movieYear + ") rating to " + movieRating)
}

func DeleteDynamoDBTables(sess session.Session) {
	svc := dynamodb.New(&sess)
	tableName := "Movies"
	movieName := "The Big New Movie"
	movieYear := "2015"

	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Year": {
				N: aws.String(movieYear),
			},
			"Title": {
				S: aws.String(movieName),
			},
		},
		TableName: aws.String(tableName),
	}

	_, err := svc.DeleteItem(input)
	if err != nil {
		fmt.Println("Got error calling DeleteItem")
		fmt.Println(err.Error())
		return
	}

	fmt.Println("Deleted '" + movieName + "' (" + movieYear + ") from table " + tableName)

}
