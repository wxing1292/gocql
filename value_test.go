package gocql

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type (
	valueSuite struct {
		suite.Suite

		keyspace string
		session  *Session
	}
)

func TestValueSuite(t *testing.T) {
	suite.Run(t, &valueSuite{})
}

func (s *valueSuite) SetupSuite() {
	s.keyspace = "test_gocql"

	cluster := NewCluster("127.0.0.1")
	cluster.ProtoVersion = 4
	cluster.Keyspace = ""
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	createKeyspace := `
		CREATE KEYSPACE IF NOT EXISTS %v WITH replication = {
			'class' : 'SimpleStrategy', 
			'replication_factor' : %d
		}
	`
	query := session.Query(fmt.Sprintf(createKeyspace, s.keyspace, 1))
	err = query.Exec()
	if err != nil {
		panic(err)
	}

	cluster.Keyspace = s.keyspace
	cluster.Timeout = 5 * time.Second
	cluster.NumConns = 8
	cluster.Consistency = LocalQuorum
	session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	s.session = session

	createTable := `
		CREATE TABLE IF NOT EXISTS traffic_cd_by_hour (
			cid text,
			mac text,
			rx bigint,
			tx bigint,
			time text,
			PRIMARY KEY (cid, time)
		)
		WITH CLUSTERING ORDER BY (time ASC);
	`
	query = s.session.Query(createTable)
	err = query.Exec()
	if err != nil {
		panic(err)
	}
}

func (s *valueSuite) TearDownSuite() {
	deleteKeyspace := `
		DROP KEYSPACE IF EXISTS %v;
	`
	query := s.session.Query(fmt.Sprintf(deleteKeyspace, s.keyspace))
	err := query.Exec()
	if err != nil {
		panic(err)
	}
}

func (s *valueSuite) SetupTest() {

}

func (s *valueSuite) TearDownTest() {

}

func (s *valueSuite) TestValue() {
	writeQuery := `
		INSERT INTO traffic_cd_by_hour (cid, mac, rx, tx, time) 
		VALUES(?, ?, ?, ?, ?)
	`
	readQuery := `
		SELECT cid, mac, rx, tx, time FROM traffic_cd_by_hour 
		WHERE cid = ? AND time = ?
	`
	cid := "594b408c-0487-11e8-9396-0dd3030097e0"
	mac := "c0:48:e6:2b:e3:eb"
	rx := int64(774141615)
	tx := int64(13443213)

	numThread := 2048
	numOp := 128
	startWaitGroup := sync.WaitGroup{}
	endWaitGroup := sync.WaitGroup{}
	startWaitGroup.Add(numThread)
	endWaitGroup.Add(numThread)

	prettyPrint := func(input interface{}) string {
		binary, _ := json.MarshalIndent(input, "", "  ")
		return string(binary)
	}

	fail := int32(0)

	thread := func() {
		startWaitGroup.Wait()

		for j := 0; j < numOp; j++ {
			now := uuid.New()

			err := s.session.Query(writeQuery, cid, mac, rx, tx, now).Exec()
			if err != nil {
				fmt.Printf("write err: %v\n", err)
			}

			result := make(map[string]interface{})
			query := s.session.Query(readQuery, cid, now)
			err = query.MapScan(result)
			if err != nil {
				fmt.Printf("read err: %v\n", err)
				continue
			}
			if result["rx"].(int64) != rx || result["tx"].(int64) != tx {
				fmt.Println("（╯' - ')╯ ┻━┻")
				fmt.Printf("%v\n", prettyPrint(result))
				fmt.Println("（╯' - ')╯ ┻━┻")
				atomic.StoreInt32(&fail, 1)
			}
		}

		endWaitGroup.Done()
	}
	for i := 0; i < numThread; i++ {
		startWaitGroup.Done()
		go thread()
	}
	endWaitGroup.Wait()

	if atomic.LoadInt32(&fail) == 1 {
		panic("err encountered")
	}
}
