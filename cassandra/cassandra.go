package cassandra

import (
	"github.com/bank-now/bn-common-model/common/model"
	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

const (
	CassandraAddress1 = "192.168.88.24:9042"
	CassandraAddress2 = "192.168.88.28:9042"
	KeySpace          = "banknow"
)

func Connect() (session *gocql.Session, err error) {
	cluster := gocql.NewCluster(CassandraAddress1, CassandraAddress2)
	cluster.Keyspace = KeySpace
	return cluster.CreateSession()

}

func Write(session *gocql.Session, transaction model.Transaction) (err error) {

	//TODO: convert. Maybe use a different type in model.Transaction
	amt := inf.NewDec(250000.00, 0)

	if err = session.Query(`INSERT INTO transactions (id, account, amt, ts) VALUES (?, ?, ?, ?)`,
		transaction.ID, transaction.AccountID, amt, transaction.Timestamp).Exec(); err != nil {
	}
	return
}

//func main() {
//	cluster := gocql.NewCluster("192.168.88.24:9042", "192.168.88.28:9042")
//	cluster.Keyspace = "banknow"
//
//	session, err := cluster.CreateSession()
//	if err != nil {
//		panic(err)
//	}
//	defer session.Close()
//
//	for i := 1; i <= 1000; i++ {
//		measureGetOne(session, fmt.Sprint(i))
//
//	}
//
//
//}
//
//func measureGetOne(session *gocql.Session, idIn string) {
//	var id string
//	start := time.Now()
//	iter := session.Query(`SELECT id FROM transactions where id=?`, idIn).Iter()
//	for iter.Scan(&id) {
//		fmt.Println("Account:", id)
//	}
//	fmt.Println("That took", time.Since(start))
//	if err := iter.Close(); err != nil {
//		log.Fatal(err)
//	}
//
//}
