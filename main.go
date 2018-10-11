package main

import (
	"encoding/json"
	"github.com/bank-now/bn-common-io/queues/sub"
	"github.com/bank-now/bn-common-model/common/model"
	"github.com/bank-now/bn-common-model/common/operation"
	"github.com/bank-now/bn-writer/cassandra"
	"github.com/gocql/gocql"
	"log"
	"time"
)

const (
	Grest   = "http://192.168.88.24:3001/"
	Name    = "writer"
	Version = "v1"
	Address = "192.168.88.24:4150"
)

var (
	session *gocql.Session
)

func main() {

	//Connect to the DB
	s, err := cassandra.Connect()
	if err != nil {
		log.Fatal(err)
	}
	session = s

	config := sub.Config{
		Address: Address,
		Name:    Name,
		Version: Version,
		Topic:   operation.WriteOperationV1Topic,
		F:       handle}
	sub.Subscribe(config)

}

func handle(b []byte) {

	//TODO: measure timing - Zipkin
	start := time.Now()

	//Read operation
	var item operation.WriteOperationV1
	err := json.Unmarshal(b, &item)
	if err != nil {
		//TODO: Deal letter queue!
		return
	}

	//Transaction
	var transaction model.Transaction
	err = json.Unmarshal(item.Item, &transaction)
	if err != nil {
		//TODO: Deal letter queue!
		return
	}
	write(transaction)
	time.Since(start)

}

func write(transaction model.Transaction) (err error) {
	start := time.Now()
	err = cassandra.Write(session, transaction)
	time.Since(start)
	if err != nil {
		//TODO: Deal letter queue!
		return
	}
	time.Since(start)
	return
}
