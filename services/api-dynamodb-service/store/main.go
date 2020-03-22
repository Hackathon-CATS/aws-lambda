package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"net/http"
	"fmt"
)


var db = dynamodb.New(session.New(), aws.NewConfig().WithRegion("eu-west-3"))
var errorLogger = log.New(os.Stderr, "ERROR ", log.Llongfile)

type item struct {
	LieuID   string `json:"lieuId"`
	EtatDesStocksPourcent   string `json:"etatDesStocksPourcent,omitempty"`
	Ouvert   bool `json:"ouvert,string"`
	Latitude   string `json:"latitude,omitempty"`
	Longitude   string `json:"longitude,omitempty"`
	OsmNodeID   string `json:"osmNodeId,omitempty"`
	TempsDAttente   string `json:"tempsDAttente,omitempty"`
	PortDesGants   bool `json:"portDesGants,string"`
	PortDuMasque   bool `json:"portDuMasque,string"`
	RespectDesDistances   bool `json:"respectDesDistances,string"`
}

func getItem(nodeId string) (*item, error) {
	// Prepare the input for the query.
	input := &dynamodb.GetItemInput{
		TableName: aws.String("Lieu"),
		Key: map[string]*dynamodb.AttributeValue{
			"LieuId": {
				S: aws.String(nodeId),
			},
		},
	}

	// Retrieve the item from DynamoDB. If no matching item is found
	// return nil.
	result, err := db.GetItem(input)
	if err != nil {
		return nil, err
	}
	if result.Item == nil {
		return nil, nil
	}

	// The result.Item object returned has the underlying type
	// map[string]*AttributeValue. We can use the UnmarshalMap helper
	// to parse this straight into the fields of a struct. Note:
	// UnmarshalListOfMaps also exists if you are working with multiple
	// items.
	it := new(item)
	err = dynamodbattribute.UnmarshalMap(result.Item, it)
	if err != nil {
		return nil, err
	}

	return it, nil
}

func putItem(it *item) error {
	input := &dynamodb.PutItemInput{
		TableName: aws.String("Lieu"),
		Item: map[string]*dynamodb.AttributeValue{
			"LieuId": {
				S: aws.String(it.LieuID),
			},
			"EtatDesStocksPourcent": {
				S: aws.String(it.EtatDesStocksPourcent),
			},
			"Ouvert": {
				BOOL: aws.Bool(it.Ouvert),
			},
			"Latitude": {
				S: aws.String(it.Latitude),
			},
			"Longitude": {
				S: aws.String(it.Longitude),
			},
			"OsmNodeId": {
				S: aws.String(it.OsmNodeID),
			},
			"TempsDAttente": {
				S: aws.String(it.TempsDAttente),
			},
			"PortDesGants": {
				BOOL: aws.Bool(it.PortDesGants),
			},
			"PortDuMasque": {
				BOOL: aws.Bool(it.PortDuMasque),
			},
			"RespectDesDistances": {
				BOOL: aws.Bool(it.RespectDesDistances),
			},
		},
	}

	_, err := db.PutItem(input)
	return err
}


func router(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	switch req.HTTPMethod {
	case "OPTIONS":
		return events.APIGatewayProxyResponse{
			StatusCode: 200,
			Headers:    map[string]string{"Access-Control-Allow-Origin": "*"},
		}, nil
	case "GET":
		return get(req)
	case "POST":
		return add(req)
	default:
		return clientError(http.StatusMethodNotAllowed)
	}
}

func get(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	nodeId := request.QueryStringParameters["nodeId"]
	fmt.Println("nodeId: ", nodeId)

	r, err := getItem(nodeId)
	if err != nil {
		serverError(err)
	}

	jsonItem, _ := json.Marshal(r)
	stringItem := string(jsonItem) + "\n"
	fmt.Println("Found item: ", stringItem)
	return events.APIGatewayProxyResponse{Body: stringItem, StatusCode: 200}, nil
}

func add(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	if req.Headers["content-type"] != "application/json" && req.Headers["Content-Type"] != "application/json" {
		return clientError(http.StatusNotAcceptable)
	}

	it := new(item)
	err := json.Unmarshal([]byte(req.Body), it)
	if err != nil {
		return clientError(http.StatusUnprocessableEntity)
	}

	err = putItem(it)
	if err != nil {
		return serverError(err)
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers:    map[string]string{"Access-Control-Allow-Origin": "*"},
	}, nil
}

func serverError(err error) (events.APIGatewayProxyResponse, error) {
	errorLogger.Println(err.Error())

	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusInternalServerError,
		Body:       http.StatusText(http.StatusInternalServerError),
	}, nil
}

func clientError(status int) (events.APIGatewayProxyResponse, error) {
	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Body:       http.StatusText(status),
	}, nil
}

func main() {
	lambda.Start(router)
}
