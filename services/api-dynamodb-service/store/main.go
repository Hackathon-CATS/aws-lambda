package main

import (
	"encoding/json"
	"log"
	"os"
	"crypto/md5"
	"encoding/hex"
	"time"

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
	HeureDerniereContribution string `json:"heureDerniereContribution"`
	DateDeContribution string `json:"dateDeContribution"`
}

type shopDB struct {
	ShopID   string
	EtatDuStock   string `json:"EtatDuStock"`
	Ouvert   bool `json:"Ouvert"`
	OSMNodeID   string `json:"OSMNodeId"`
	TempsAttente   int64 `json:"TempsAttente"`
	PortDesGants   int64 `json:"PortDeGants"`
	PortDuMasque   int64 `json:"PortDeMasque"`
	RespectDesDistances   int64 `json:"RespectDesDistances"`
	NombreDeContribution int64 `json:"NombreDeContribution"`
	HeureDerniereContribution string `json:"HeureDerniereContribution"`
	DateDeContribution string `json:"DateDeContribution"`
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
	HeureDerniereContribution string `json:":hdc"`
	DateDeContribution string `json:":ddc"`
	Zero int64 `json:":zero"`
}

func shopDBToShopDBInc(s *shopDB) *shopDBInc {
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
		Zero: 0,
	}
}

func shopDBToShop(s *shopDB) *shop {
	fmt.Println(s.ShopID)
	fmt.Println(s.OSMNodeID)
	fmt.Println(s.NombreDeContribution)
	seuil := s.NombreDeContribution / 2
	return &shop{
		ShopID: s.ShopID,
		EtatDesStocks: s.EtatDuStock,
		Ouvert: s.Ouvert,
		OSMNodeID: s.OSMNodeID,
		TempsAttente: int64(s.TempsAttente / s.NombreDeContribution),
		PortDesGants: s.PortDesGants > seuil,
		PortDuMasque: s.PortDuMasque > seuil,
		RespectDesDistances: s.RespectDesDistances > seuil,
		NombreDeContribution: s.NombreDeContribution,
		HeureDerniereContribution: s.HeureDerniereContribution,
		DateDeContribution: s.DateDeContribution,
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
		UpdateExpression:          aws.String("set TempsAttente = if_not_exists(TempsAttente, :zero) + :ta, " +
			"PortDuMasque = if_not_exists(PortDuMasque, :zero) + :pdm, " +
			"PortDesGants = if_not_exists(PortDesGants, :zero) + :pdg, " +
			"RespectDesDistances = if_not_exists(RespectDesDistances, :zero) + :rdd, " +
			"Ouvert = :o, " +
			"EtatDuStock = :eds," +
			"NombreDeContribution = if_not_exists(NombreDeContribution, :zero) + :ndc," +
			"HeureDerniereContribution = :hdc," +
			"OSMNodeId = :oni," +
			"DateDeContribution = :ddc," +
			"id = :si"),
		ExpressionAttributeValues: data,
	}

	_, err = db.UpdateItem(input)

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

	id := fmt.Sprintf("%s#%s", time.Now().Format("02-01-2006"), nodeId)
	fmt.Println("md5: "+ GetMD5Hash(id))
	r, err := getShop(GetMD5Hash(id))
	if err != nil {
		serverError(err)
	}


	if *r == (shopDB{}) {
		return events.APIGatewayProxyResponse{Body: "{}", StatusCode: 200, Headers:    map[string]string{"Access-Control-Allow-Origin": "*"}}, nil
	}
	s := shopDBToShop(r)
	jsonItem, _ := json.Marshal(s)
	stringItem := string(jsonItem) + "\n"
	fmt.Println("Found item: ", stringItem)
	return events.APIGatewayProxyResponse{Body: stringItem, StatusCode: 200, Headers:    map[string]string{"Access-Control-Allow-Origin": "*"}}, nil
}

func add(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

	if req.Headers["content-type"] != "application/json" && req.Headers["Content-Type"] != "application/json" {
		return clientError(http.StatusNotAcceptable)
	}

	s := new(shop)
	err := json.Unmarshal([]byte(req.Body), s)
	if err != nil {
		return clientError(http.StatusUnprocessableEntity)
	}
	id := fmt.Sprintf("%s#%s", time.Now().Format("02-01-2006"), s.OSMNodeID)
	s.ShopID = GetMD5Hash(id)
	s.DateDeContribution = time.Now().Format("02-01-2006")
	s.HeureDerniereContribution = time.Now().Format("03:04:05")

	err = putShop(shopToShopDB(s))

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

func GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}
