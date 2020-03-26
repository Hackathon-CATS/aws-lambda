package main

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws/awserr"
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

type shop struct {
	ShopID   string `json:"shopId"`
	EtatDesStocks   string `json:"etatDesStocks"`
	Ouvert   bool `json:"ouvert"`
	OSMNodeID   string `json:"osmNodeId"`
	TempsAttente   int64 `json:"tempsAttente"`
	PortDesGants   bool `json:"portDesGants"`
	PortDuMasque   bool `json:"portDuMasque"`
	RespectDesDistances   bool `json:"respectDesDistances"`
	NombreDeContribution int64 `json:"nombreDeContribution"`
	SessionNombreDeContribution int64 `json:"sessionNombreDeContribution"`
	HeureDerniereContribution string `json:"heureDerniereContribution"`
	DateDeContribution string `json:"dateDeContribution"`
}

type shopDB struct {
	ShopID   string
	EtatDuStock   string `json:"EtatDuStock"`
	Ouvert   bool `json:"Ouvert"`
	OSMNodeID   string `json:"OSMNodeId"`
	TempsAttente   int64 `json:"TempsAttente"`
	PortDesGants   int64 `json:"PortDesGants"`
	PortDuMasque   int64 `json:"PortDuMasque"`
	RespectDesDistances   int64 `json:"RespectDesDistances"`
	NombreDeContribution int64 `json:"NombreDeContribution"`
	SessionNombreDeContribution int64 `json:"SessionNombreDeContribution"`
	HeureDerniereContribution string `json:"HeureDerniereContribution"`
	DateDeContribution string `json:"DateDeContribution"`
	TimestampDerniereContribution int64 `json:"TimestampDerniereContribution"`
}

type shopDBInc struct {
	ShopID   string `json:":si"`
	EtatDuStock   string `json:":eds"`
	Ouvert   bool `json:":o"`
	OSMNodeID   string `json:":oni"`
	TempsAttente   int64 `json:":ta"`
	PortDesGants   int64 `json:":pdg"`
	PortDuMasque   int64 `json:":pdm"`
	RespectDesDistances   int64 `json:":rdd"`
	NombreDeContribution int64 `json:":ndc"`
	SessionNombreDeContribution int64 `json:":sndc"`
	HeureDerniereContribution string `json:":hdc"`
	DateDeContribution string `json:":ddc"`
	TimestampDerniereContribution int64 `json:":tdc"`
	MinTimestampDerniereContribution int64 `json:":mtdc"`
	MaxSessionNombreDeContribution int64 `json:":msndc"`
	Zero int64 `json:":zero"`
}

func shopDBToShopDBInc(s *shopDB) *shopDBInc {
	loc, _ := time.LoadLocation("Europe/Paris")
	return &shopDBInc{
		ShopID: s.ShopID,
		EtatDuStock: s.EtatDuStock,
		Ouvert: s.Ouvert,
		OSMNodeID: s.OSMNodeID,
		TempsAttente: s.TempsAttente,
		PortDesGants: s.PortDesGants,
		PortDuMasque: s.PortDuMasque,
		RespectDesDistances: s.RespectDesDistances,
		NombreDeContribution: 1,
		HeureDerniereContribution: s.HeureDerniereContribution,
		DateDeContribution: s.DateDeContribution,
		TimestampDerniereContribution: s.TimestampDerniereContribution,
		SessionNombreDeContribution: 1,
		MinTimestampDerniereContribution: time.Now().Add(-1*time.Hour).In(loc).Unix(),
		MaxSessionNombreDeContribution: 20,
		Zero: 0,
	}
}

func shopDBToShop(s *shopDB) *shop {
	seuil := s.SessionNombreDeContribution / 2
	return &shop{
		ShopID: s.ShopID,
		EtatDesStocks: s.EtatDuStock,
		Ouvert: s.Ouvert,
		OSMNodeID: s.OSMNodeID,
		TempsAttente: int64(s.TempsAttente / s.SessionNombreDeContribution),
		PortDesGants: s.PortDesGants > seuil,
		PortDuMasque: s.PortDuMasque > seuil,
		RespectDesDistances: s.RespectDesDistances > seuil,
		NombreDeContribution: s.NombreDeContribution,
		HeureDerniereContribution: s.HeureDerniereContribution,
		DateDeContribution: s.DateDeContribution,
		SessionNombreDeContribution: s.SessionNombreDeContribution,
	}
}

func Btoi(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func shopToShopDB(s *shop) *shopDB {
	return &shopDB{
		ShopID: s.ShopID,
		EtatDuStock: s.EtatDesStocks,
		Ouvert: s.Ouvert,
		OSMNodeID: s.OSMNodeID,
		TempsAttente: s.TempsAttente,
		PortDesGants: Btoi(s.PortDesGants),
		PortDuMasque: Btoi(s.PortDuMasque),
		RespectDesDistances: Btoi(s.RespectDesDistances),
		NombreDeContribution: 1,
		HeureDerniereContribution: s.HeureDerniereContribution,
		DateDeContribution: s.DateDeContribution,
		SessionNombreDeContribution: 1,
	}
}

func getShop(shopId string) (*shopDB, error) {
	// Prepare the input for the query.
	input := &dynamodb.GetItemInput{
		TableName: aws.String("Shop"),
		Key: map[string]*dynamodb.AttributeValue{
			"ShopId": {
				S: aws.String(shopId),
			},
		},
	}



	result, err := db.GetItem(input)
	if err != nil {
		return nil, err
	}


	s := new(shopDB)
	err = dynamodbattribute.UnmarshalMap(result.Item, s)

	if err != nil {
		return nil, err
	}

	return s, nil
}

func putShop(s *shopDB) error {
	data, err := dynamodbattribute.MarshalMap(shopDBToShopDBInc(s))
	if err != nil {
 		return err
	}
	input := &dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"ShopId": {
				S: aws.String(s.ShopID),
			},
		},
		TableName:                 aws.String("Shop"),
		UpdateExpression:          aws.String("set TempsAttente = :ta, " +
			"PortDuMasque = :pdm, " +
			"PortDesGants = :pdg, " +
			"RespectDesDistances = :rdd, " +
			"Ouvert = :o, " +
			"EtatDuStock = :eds," +
			"NombreDeContribution = if_not_exists(NombreDeContribution, :zero) + :ndc," +
			"SessionNombreDeContribution = :sndc," +
			"HeureDerniereContribution = :hdc," +
			"OSMNodeId = :oni," +
			"DateDeContribution = :ddc," +
			"TimestampDerniereContribution = :tdc," +
			"id = :si"),
		ExpressionAttributeValues: data,
		ConditionExpression: aws.String("TimestampDerniereContribution < :mtdc OR SessionNombreDeContribution >= :msndc"),
	}

	_, err = db.UpdateItem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				err = nil
				input := &dynamodb.UpdateItemInput{
					Key: map[string]*dynamodb.AttributeValue{
						"ShopId": {
							S: aws.String(s.ShopID),
						},
					},
					TableName:                 aws.String("Shop"),
					UpdateExpression:          aws.String("set TempsAttente = if_not_exists(TempsAttente, :zero) + :ta, " +
						"PortDuMasque = if_not_exists(PortDuMasque, :zero) + :pdm, " +
						"PortDesGants = if_not_exists(PortDesGants, :zero) + :pdg, " +
						"RespectDesDistances = if_not_exists(RespectDesDistances, :zero) + :rdd, " +
						"Ouvert = :o, " +
						"EtatDuStock = :eds," +
						"NombreDeContribution = if_not_exists(NombreDeContribution, :zero) + :ndc," +
						"SessionNombreDeContribution = if_not_exists(SessionNombreDeContribution, :zero) + :sndc," +
						"HeureDerniereContribution = :hdc," +
						"OSMNodeId = :oni," +
						"DateDeContribution = :ddc," +
						"TimestampDerniereContribution = :tdc," +
						"id = :si"),
					ExpressionAttributeValues: data,
					ConditionExpression: aws.String(":mtdc  > :msndc"),
				}
				_, err = db.UpdateItem(input)
			}

		}
	}

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
	params := request.QueryStringParameters
	if len(params) == 0 {
		clientError(http.StatusBadRequest)
	}

	nodeId, ok := params["OSMNodeId"]
	if !ok {
		clientError(http.StatusBadRequest)
	}

	r, err := getShop(nodeId)
	if err != nil {
		serverError(err)
	}


	if *r == (shopDB{}) {
		return events.APIGatewayProxyResponse{Body: "{}", StatusCode: 200, Headers:    map[string]string{"Access-Control-Allow-Origin": "*"}}, nil
	}
	s := shopDBToShop(r)
	jsonItem, _ := json.Marshal(s)
	stringItem := string(jsonItem) + "\n"
	return events.APIGatewayProxyResponse{Body: stringItem, StatusCode: 200, Headers:    map[string]string{"Access-Control-Allow-Origin": "*"}}, nil
}

func add(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	loc, _ := time.LoadLocation("Europe/Paris")

	if req.Headers["content-type"] != "application/json" && req.Headers["Content-Type"] != "application/json" {
		return clientError(http.StatusNotAcceptable)
	}

	s := new(shop)
	err := json.Unmarshal([]byte(req.Body), s)
	if err != nil {
		return clientError(http.StatusUnprocessableEntity)
	}
	s.ShopID = s.OSMNodeID
	s.DateDeContribution = time.Now().In(loc).Format("02-01-2006")
	s.HeureDerniereContribution = time.Now().In(loc).Format("15:04:05")

	sDB := shopToShopDB(s)
	sDB.TimestampDerniereContribution = time.Now().In(loc).Unix()

	err = putShop(sDB)

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
