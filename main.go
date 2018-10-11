package main

import (
	"encoding/json"
	"fmt"
	"github.com/bank-now/bn-common-io/queues/sub"
	"github.com/bank-now/bn-common-model/common/operation"
	"github.com/bank-now/bn-writer/cassandra"
)

const (
	Grest   = "http://192.168.88.24:3001/"
	Name    = "writer"
	Version = "v1"
	Address = "192.168.88.24:4150"
)

func main() {

	//Connect to the DB
	session := cassandra.Connect()

	config := sub.Config{
		Address: Address,
		Name:    Name,
		Version: Version,
		Topic:   operation.WriteOperationV1Topic,
		F:       handle}
	sub.Subscribe(config)

}

func handle(b []byte) {
	var item operation.WriteOperationV1
	err := json.Unmarshal(b, &item)
	if err != nil {
		//Deal letter queue!
		return
	}
	//url := fmt.Sprint(Grest, model.TransactionTable)
	//rBody, err := rest.Post(url, item.Item)
	fmt.Println(string(item.Item))

}
