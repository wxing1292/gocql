package gocql

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type (
	missingSuite struct {
		suite.Suite

		keyspace string
		session  *Session
	}
)

func TestMissingSuite(t *testing.T) {
	suite.Run(t, &missingSuite{})
}

func (s *missingSuite) SetupSuite() {
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
	cluster.Timeout = 10 * time.Second
	cluster.NumConns = 8
	cluster.Consistency = LocalQuorum
	session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	s.session = session

	createNodeTable := `
		CREATE TABLE history_node (
		  tree_id           uuid,
		  branch_id         uuid,
		  node_id           bigint, -- node_id: first eventID in a batch of events
		  txn_id            bigint, -- for override the same node_id: bigger txn_id wins
		  data                blob, -- Batch of workflow execution history events as a blob
		  data_encoding       text, -- Protocol used for history serialization
		  PRIMARY KEY ((tree_id), branch_id, node_id, txn_id )
		) WITH CLUSTERING ORDER BY (branch_id ASC, node_id ASC, txn_id DESC)
		  AND COMPACTION = {
			 'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
		};
	`
	query = s.session.Query(createNodeTable)
	err = query.Exec()
	if err != nil {
		panic(err)
	}

	createTreeTable := `
		CREATE TABLE history_tree (
		  tree_id               uuid,
		  branch_id             uuid,
		  branch                blob,
		  branch_encoding       text,
		  PRIMARY KEY ((tree_id), branch_id )
		) WITH COMPACTION = {
			'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
		};
	`
	query = s.session.Query(createTreeTable)
	err = query.Exec()
	if err != nil {
		panic(err)
	}

	createExecutionTable := `
		CREATE TABLE executions (
		  shard_id                       int,
		  type                           int, -- enum RowType { Shard, Execution, TransferTask, TimerTask, ReplicationTask, VisibilityTask}
		  namespace_id                   uuid,
		  workflow_id                    text,
		  run_id                         uuid,
		  visibility_ts                  timestamp, -- unique identifier for timer tasks for an execution
		  task_id                        bigint, -- unique identifier for transfer and timer tasks for an execution
		  execution                      blob,
		  execution_encoding             text,
		  range_id                       bigint,
		  PRIMARY KEY  (shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id)
		) WITH COMPACTION = {
			'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
		  };
	`
	query = s.session.Query(createExecutionTable)
	err = query.Exec()
	if err != nil {
		panic(err)
	}
}

func (s *missingSuite) TearDownSuite() {
	deleteKeyspace := `
		DROP KEYSPACE IF EXISTS %v;
	`
	query := s.session.Query(fmt.Sprintf(deleteKeyspace, s.keyspace))
	err := query.Exec()
	if err != nil {
		panic(err)
	}
}

func (s *missingSuite) SetupTest() {

}

func (s *missingSuite) TearDownTest() {

}

func (s *missingSuite) TestMissing() {
	queryInsertTree := `INSERT INTO history_tree ` +
		`(tree_id, branch_id, branch, branch_encoding) ` +
		`VALUES (?, ?, ?, ?) `

	queryInsertNode := `INSERT INTO history_node ` +
		`(tree_id, branch_id, node_id, txn_id, data, data_encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?) `

	querySelectTree := `SELECT branch, branch_encoding FROM history_tree ` +
		`WHERE tree_id = ? AND branch_id = ? `

	querySelectNode := `SELECT data, data_encoding FROM history_node ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id = ?`

	queryInsertExecution := `INSERT INTO executions ` +
		`(shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id, execution, execution_encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	queryCreateShard := `INSERT INTO executions ` +
		`(shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id, range_id) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	queryAssertShard := `UPDATE executions ` +
		`SET range_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	shardID := int64(2333)
	namespaceID := uuid.New()
	visibilityTime := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC).Truncate(time.Millisecond).UTC()
	taskID := -10

	numThread := 16
	numOp := 128
	startWaitGroup := sync.WaitGroup{}
	endWaitGroup := sync.WaitGroup{}
	startWaitGroup.Add(numThread)
	endWaitGroup.Add(numThread)

	//prettyPrint := func(input interface{}) string {
	//	binary, _ := json.MarshalIndent(input, "", "  ")
	//	return string(binary)
	//}

	fail := int32(0)

	shardNamespaceID := "10000000-1000-f000-f000-000000000000"
	shardWorkflowID := "20000000-1000-f000-f000-000000000000"
	shardRunID := "30000000-1000-f000-f000-000000000000"
	shardRangeID := 100
	batch := s.session.NewBatch(LoggedBatch)
	batch.Query(queryCreateShard, shardID, 0, shardNamespaceID, shardWorkflowID, shardRunID, visibilityTime, taskID, shardRangeID)
	result := make(map[string]interface{})
	applied, _, err := s.session.MapExecuteBatchCAS(batch, result)
	if err != nil {
		panic(err)
	}
	if !applied {
		panic("unable to initialize shard")
	}

	thread := func() {
		startWaitGroup.Wait()

		for j := 0; j < numOp; j++ {
			workflowID := "workflow_id_" + uuid.New()
			runID := uuid.New()
			treeID := runID
			branchID := uuid.New()

			batch := s.session.NewBatch(LoggedBatch)
			batch.Query(queryInsertTree, treeID, branchID, "tree payload", "encoding")
			batch.Query(queryInsertNode, treeID, branchID, 1, 0, "node payload", "encoding")
			if err := s.session.ExecuteBatch(batch); err != nil {
				fmt.Printf("write tree & node err: %v\n", err)
				continue
			}

			batch = s.session.NewBatch(LoggedBatch)
			batch.Query(queryInsertExecution, shardID, 1, namespaceID, workflowID, runID, visibilityTime, taskID, "execution payload", "encoding")
			batch.Query(queryAssertShard, shardRangeID, shardID, 0, shardNamespaceID, shardWorkflowID, shardRunID, visibilityTime, taskID, shardRangeID)
			result := make(map[string]interface{})
			applied, _, err := s.session.MapExecuteBatchCAS(batch, result)
			if err != nil || !applied {
				fmt.Printf("write execution err: %v - %v\n", err, applied)
				continue
			}

			result = make(map[string]interface{})
			query := s.session.Query(querySelectTree, treeID, branchID)
			err = query.MapScan(result)
			switch err {
			case nil:
				// noop
			case ErrNotFound:
				// smoking gun
				fmt.Println("（╯' - ')╯ ┻━┻")
				fmt.Printf("missing tree: %v - %v\n", treeID, branchID)
				fmt.Println("（╯' - ')╯ ┻━┻")
				atomic.StoreInt32(&fail, 1)
			default:
				fmt.Printf("unable to get tree: %v\n", err)
			}

			result = make(map[string]interface{})
			query = s.session.Query(querySelectNode, treeID, branchID, 1)
			err = query.MapScan(result)
			switch err {
			case nil:
				// noop
			case ErrNotFound:
				// smoking gun
				fmt.Println("（╯' - ')╯ ┻━┻")
				fmt.Printf("missing node: %v - %v\n", treeID, branchID)
				fmt.Println("（╯' - ')╯ ┻━┻")
				atomic.StoreInt32(&fail, 1)
			default:
				fmt.Printf("unable to get node: %v\n", err)
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
