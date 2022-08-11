package multipaxos

import (
	"context"
	"github.com/psu-csl/replicated-store/go/config"
	pb "github.com/psu-csl/replicated-store/go/consensus/multipaxos/comm"
	"github.com/psu-csl/replicated-store/go/consensus/multipaxos/util"
	"github.com/psu-csl/replicated-store/go/log"
	"github.com/psu-csl/replicated-store/go/store"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"testing"
	"time"
)

const NumPeers = 3

var (
	configs = make([]config.Config, NumPeers)
	logs    = make([]*log.Log, NumPeers)
	peers   = make([]*Multipaxos, NumPeers)
	stores  = make([]*store.MemKVStore, NumPeers)
)

func setupPeers() {
	for i:= int64(0); i < NumPeers; i++ {
		configs[i] = config.DefaultConfig(i, NumPeers)
		logs[i] = log.NewLog()
		stores[i] = store.NewMemKVStore()
		peers[i] = NewMultipaxos(configs[i], logs[i])
	}
}

func setupServer() {
	setupPeers()
	peers[0].Start()
	peers[1].Start()
	peers[2].Start()
}

func setupOnePeer(id int64) {
	configs[id] = config.DefaultConfig(id, NumPeers)
	logs[id] = log.NewLog()
	stores[id] = store.NewMemKVStore()
	peers[id] = NewMultipaxos(configs[id], logs[id])
}

func tearDown() {
	for _, peer := range peers {
		peer.Stop()
	}
}

func TestNewMultipaxos(t *testing.T) {
	setupOnePeer(0)

	assert.Equal(t, MaxNumPeers, LeaderByPeer(peers[0]))
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.False(t, IsSomeoneElseLeaderByPeer(peers[0]))
}

func TestNextBallot(t *testing.T) {
	setupOnePeer(2)
	id := peers[2].Id()
	ballot := id

	ballot += RoundIncrement
	assert.Equal(t, ballot, peers[2].NextBallot())
	ballot += RoundIncrement
	assert.Equal(t, ballot, peers[2].NextBallot())

	assert.True(t, IsLeaderByPeer(peers[2]))
	assert.False(t, IsSomeoneElseLeaderByPeer(peers[2]))
	assert.Equal(t, id, LeaderByPeer(peers[2]))
}

func TestRequestsWithLowerBallotIgnored(t *testing.T) {
	setupOnePeer(0)
	setupOnePeer(1)
	peers[0].StartServer()
	stub := makeStub(configs[0].Peers[0])

	peers[0].NextBallot()
	peers[0].NextBallot()
	staleBallot := peers[1].NextBallot()

	r1 := sendHeartbeat(stub, staleBallot, 0, 0)
	assert.EqualValues(t, pb.ResponseType_REJECT, r1.GetType())
	assert.True(t, IsLeaderByPeer(peers[0]))

	r2 := sendPrepare(stub, staleBallot)
	assert.EqualValues(t, pb.ResponseType_REJECT, r2.GetType())
	assert.True(t, IsLeaderByPeer(peers[0]))

	index := logs[0].AdvanceLastIndex()
	instance := util.MakeInstance(staleBallot, index)
	r3 := sendAccept(stub, instance)
	assert.EqualValues(t, pb.ResponseType_REJECT, r3.GetType())
	assert.True(t, IsLeaderByPeer(peers[0]))
	assert.Nil(t, logs[0].Find(index))

	peers[0].Stop()
}

func TestRequestsWithHigherBallotChangeLeaderToFollower(t *testing.T) {
	setupOnePeer(0)
	setupOnePeer(1)
	peers[0].StartServer()
	stub := makeStub(configs[0].Peers[0])

	peers[0].NextBallot()
	assert.True(t, IsLeaderByPeer(peers[0]))
	r1 := sendHeartbeat(stub, peers[1].NextBallot(), 0, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r1.GetType())
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	peers[0].NextBallot()
	assert.True(t, IsLeaderByPeer(peers[0]))
	r2 := sendPrepare(stub, peers[1].NextBallot())
	assert.EqualValues(t, pb.ResponseType_OK, r2.GetType())
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	peers[0].NextBallot()
	assert.True(t, IsLeaderByPeer(peers[0]))
	index := logs[0].AdvanceLastIndex()
	instance := util.MakeInstance(peers[1].NextBallot(), index)
	r3 := sendAccept(stub, instance)
	assert.EqualValues(t, pb.ResponseType_OK, r3.GetType())
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	peers[0].Stop()
}

func TestNextBallotAfterHeartbeat(t *testing.T) {
	setupPeers()
	peers[0].StartServer()
	stub := makeStub(configs[0].Peers[0])
	ballot := peers[0].Id()

	ctx := context.Background()
	request := pb.HeartbeatRequest{
		Ballot: peers[1].NextBallot(),
	}
	stub.Heartbeat(ctx, &request)
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	ballot += RoundIncrement
	ballot += RoundIncrement
	assert.EqualValues(t, ballot, peers[0].NextBallot())

	peers[0].Stop()
}

func TestHeartbeatCommitsAndTrims(t *testing.T) {
	setupOnePeer(0)
	peers[0].StartServer()
	stub := makeStub(configs[0].Peers[0])

	ballot := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	logs[0].Append(util.MakeInstance(ballot, index1))
	index2 := logs[0].AdvanceLastIndex()
	logs[0].Append(util.MakeInstance(ballot, index2))
	index3 := logs[0].AdvanceLastIndex()
	logs[0].Append(util.MakeInstance(ballot, index3))

	r1 := sendHeartbeat(stub, ballot, index2, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r1.GetType())
	assert.EqualValues(t, 0, r1.LastExecuted)
	assert.True(t, log.IsCommitted(logs[0].Find(index1)))
	assert.True(t, log.IsCommitted(logs[0].Find(index2)))
	assert.True(t, log.IsInProgress(logs[0].Find(index3)))

	logs[0].Execute(stores[0])
	logs[0].Execute(stores[0])

	r2 := sendHeartbeat(stub, ballot, index2, index2)
	assert.EqualValues(t, pb.ResponseType_OK, r2.GetType())
	assert.EqualValues(t, index2, r2.LastExecuted)
	assert.Nil(t, logs[0].Find(index1))
	assert.Nil(t, logs[0].Find(index2))
	assert.True(t, log.IsInProgress(logs[0].Find(index3)))

	peers[0].Stop()
}

func TestPrepareRespondsWithCorrectInstances(t *testing.T) {
	setupOnePeer(0)
	peers[0].StartServer()
	stub := makeStub(configs[0].Peers[0])

	ballot := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	instance1 := util.MakeInstance(ballot, index1)
	logs[0].Append(instance1)
	index2 := logs[0].AdvanceLastIndex()
	instance2 := util.MakeInstance(ballot, index2)
	logs[0].Append(instance2)
	index3 := logs[0].AdvanceLastIndex()
	instance3 := util.MakeInstance(ballot, index3)
	logs[0].Append(instance3)

	r1 := sendPrepare(stub, ballot)
	assert.EqualValues(t, pb.ResponseType_OK, r1.GetType())
	assert.EqualValues(t, 3, len(r1.GetLogs()))
	assert.True(t, log.IsEqualInstance(instance1, r1.GetLogs()[0]))
	assert.True(t, log.IsEqualInstance(instance2, r1.GetLogs()[1]))
	assert.True(t, log.IsEqualInstance(instance3, r1.GetLogs()[2]))

	r2 := sendHeartbeat(stub, ballot, index2, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r2.GetType())
	logs[0].Execute(stores[0])
	logs[0].Execute(stores[0])

	r3 := sendPrepare(stub, ballot)
	assert.EqualValues(t, pb.ResponseType_OK, r3.GetType())
	assert.EqualValues(t, 3, len(r3.GetLogs()))
	assert.True(t, log.IsExecuted(r3.GetLogs()[0]))
	assert.True(t, log.IsExecuted(r3.GetLogs()[1]))
	assert.True(t, log.IsEqualInstance(instance3, r1.GetLogs()[2]))

	r4 := sendHeartbeat(stub, ballot, index2, 2)
	assert.EqualValues(t, pb.ResponseType_OK, r4.GetType())

	r5 := sendPrepare(stub, ballot)
	assert.EqualValues(t, pb.ResponseType_OK, r5.GetType())
	assert.EqualValues(t, 1, len(r5.GetLogs()))
	assert.True(t, log.IsEqualInstance(instance3, r5.GetLogs()[0]))

	peers[0].Stop()
}

func TestAcceptAppendsToLog(t *testing.T) {
	setupOnePeer(0)
	peers[0].StartServer()
	stub := makeStub(configs[0].Peers[0])

	ballot := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	instance1 := util.MakeInstance(ballot, index1)
	index2 := logs[0].AdvanceLastIndex()
	instance2 := util.MakeInstance(ballot, index2)

	r1 := sendAccept(stub, instance1)
	assert.EqualValues(t, pb.ResponseType_OK, r1.GetType())
	assert.True(t, log.IsEqualInstance(instance1, logs[0].Find(index1)))
	assert.Nil(t, logs[0].Find(index2))

	r2 := sendAccept(stub, instance2)
	assert.EqualValues(t, pb.ResponseType_OK, r2.GetType())
	assert.True(t, log.IsEqualInstance(instance1, logs[0].Find(index1)))
	assert.True(t, log.IsEqualInstance(instance2, logs[0].Find(index2)))

	peers[0].Stop()
}

func TestHeartbeatResponseWithHighBallotChangesLeaderToFollower(t *testing.T) {
	setupPeers()
	defer tearDown()
	for _, peer := range peers {
		peer.StartServer()
		peer.Connect()
	}
	stub1 := makeStub(configs[0].Peers[1])

	peer0Ballot := peers[0].NextBallot()
	peer2Ballot := peers[2].NextBallot()

	r := sendHeartbeat(stub1, peer2Ballot, 0, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r.GetType())
	assert.False(t, IsLeaderByPeer(peers[1]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[1]))

	assert.True(t, IsLeaderByPeer(peers[0]))
	peers[0].SendHeartbeats(peer0Ballot, 0)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[0]))
}

func TestPrepareResponseWithHighBallotChangesLeaderToFollower(t *testing.T) {
	setupPeers()
	defer tearDown()
	for _, peer := range peers {
		peer.StartServer()
		peer.Connect()
	}
	stub1 := makeStub(configs[0].Peers[1])

	peer0Ballot := peers[0].NextBallot()
	peer2Ballot := peers[2].NextBallot()

	r := sendHeartbeat(stub1, peer2Ballot, 0, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r.GetType())
	assert.False(t, IsLeaderByPeer(peers[1]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[1]))

	assert.True(t, IsLeaderByPeer(peers[0]))
	peers[0].SendPrepares(peer0Ballot)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[0]))
}

func TestAcceptResponseWithHighBallotChangesLeaderToFollower(t *testing.T) {
	setupPeers()
	defer tearDown()
	for _, peer := range peers {
		peer.StartServer()
		peer.Connect()
	}
	stub1 := makeStub(configs[0].Peers[1])

	peer0Ballot := peers[0].NextBallot()
	peer2Ballot := peers[2].NextBallot()

	r := sendHeartbeat(stub1, peer2Ballot, 0, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r.GetType())
	assert.False(t, IsLeaderByPeer(peers[1]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[1]))

	assert.True(t, IsLeaderByPeer(peers[0]))
	peers[0].SendAccepts(peer0Ballot, 1, &pb.Command{}, 0)
	assert.False(t, IsLeaderByPeer(peers[0]))
	assert.EqualValues(t, 2, LeaderByPeer(peers[0]))
}

func TestSendHeartbeats(t *testing.T) {
	setupPeers()
	defer tearDown()
	peers[0].StartServer()
	peers[1].StartServer()

	ballot := peers[0].NextBallot()
	index := logs[0].AdvanceLastIndex()
	for i, log := range logs {
		instance := util.MakeInstance(ballot, index)
		log.Append(instance)
		log.Commit(index)
		log.Execute(stores[i])
	}

	assert.EqualValues(t, 0, peers[0].SendHeartbeats(ballot, 0))

	peers[2].StartServer()
	time.Sleep(2 * time.Second)

	assert.EqualValues(t, index, peers[0].SendHeartbeats(ballot, 0))
}

func TestSendHeartbeatsWithDifferentProgress(t *testing.T) {
	setupPeers()
	defer tearDown()
	for _, peer := range peers {
		peer.StartServer()
	}
	time.Sleep(2*time.Second)

	numInstances := 10
	numHalfInstance := numInstances / 2
	ballot := peers[0].NextBallot()
	for i, log := range logs {
		for num := 0; num < numInstances; num++ {
			index := log.AdvanceLastIndex()
			instance := util.MakeInstance(ballot, index)
			log.Append(instance)
			log.Commit(index)
			if i != 2 || num < numHalfInstance {
				log.Execute(stores[i])
			}
		}
	}

	gle1 := peers[0].SendHeartbeats(ballot, 0)
	assert.EqualValues(t, numHalfInstance, gle1)

	for index := numHalfInstance; index < numInstances; index++ {
		logs[2].Execute(stores[2])
	}
	gle2 := peers[0].SendHeartbeats(ballot, gle1)
	assert.EqualValues(t, numInstances, gle2)
}

func TestSendPreparesMajority(t *testing.T) {
	setupPeers()
	peers[0].StartServer()
	defer peers[0].Stop()

	expect := make(map[int64]*pb.Instance)
	ballot := peers[0].NextBallot()
	index := logs[0].AdvanceLastIndex()
	for i, log := range logs {
		instance := util.MakeInstance(ballot, index)
		expect[index] = instance
		log.Append(instance)
		log.Commit(index)
		log.Execute(stores[i])
	}
	assert.Nil(t, peers[0].SendPrepares(ballot))

	peers[1].StartServer()
	defer peers[1].Stop()
	time.Sleep(2 * time.Second)

	logMap := peers[0].SendPrepares(ballot)
	assert.Equal(t, len(expect), len(logMap))
	for index, instance := range expect {
		assert.True(t, log.IsEqualInstance(instance, logMap[index]))
	}
}

func TestSendPreapresWithReplay(t *testing.T) {
	setupPeers()
	defer tearDown()
	for _, peer := range peers {
		peer.StartServer()
	}
	peers[2].Connect()

	ballot0 := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	instance1 := util.MakeInstance(ballot0, index1)
	logs[0].Append(instance1)
	logs[1].Append(instance1)
	logs[0].Commit(index1)

	ballot1 := peers[1].NextBallot()
	logs[1].AdvanceLastIndex()
	index2 := logs[1].AdvanceLastIndex()
	instance2 := util.MakeInstance(ballot1, index2)
	logs[0].Append(instance2)
	logs[1].Append(instance2)
	logs[2].Append(instance2)

	ballot2 := peers[2].NextBallot()
	logMap := peers[2].SendPrepares(ballot2)
	assert.EqualValues(t, 2, len(logMap))

	peers[2].Replay(ballot2, logMap)

	instance1.Ballot = ballot2
	assert.True(t, log.IsCommitted(logs[0].Find(index1)))
	assert.True(t, log.IsCommitted(logs[2].Find(index1)))
	assert.True(t, log.IsEqualInstance(instance1, logs[1].Find(index1)))

	instance2.Ballot = ballot2
	assert.True(t, log.IsCommitted(logs[2].Find(index2)))
	assert.True(t, log.IsEqualInstance(instance2, logs[0].Find(index2)))
	assert.True(t, log.IsEqualInstance(instance2, logs[1].Find(index2)))
}

func TestSendPreparesWithMerge(t *testing.T) {
	setupPeers()
	defer tearDown()
	peers[0].StartServer()
	peers[1].StartServer()
	peers[0].Connect()

	//Instances in log_0
	ballot0 := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	index2 := logs[0].AdvanceLastIndex()
	peer0I2 := util.MakeInstance(ballot0, index2)
	logs[0].Append(peer0I2)

	//Instance in log_1
	ballot1 := peers[1].NextBallot()
	peer1I1 := util.MakeInstance(ballot1, logs[1].AdvanceLastIndex())
	peer1I2 := util.MakeInstance(ballot1, logs[1].AdvanceLastIndex())
	logs[1].Append(peer1I1)
	logs[1].Commit(index1)
	logs[1].Append(peer1I2)

	leaderBallot := peers[0].NextBallot()
	logMap := peers[0].SendPrepares(leaderBallot)
	assert.EqualValues(t, 2, len(logMap))
	assert.EqualValues(t, ballot1, logMap[index1].GetBallot())
	assert.True(t, log.IsCommitted(logMap[index1]))
	assert.EqualValues(t, ballot1, logMap[index2].GetBallot())
	assert.True(t, log.IsInProgress(logMap[index2]))

	peers[2].StartServer()
	peers[0].Connect()

	// Instances in log_2
	ballot2 := peers[2].NextBallot()
	peer2I1 := util.MakeInstance(ballot2, logs[2].AdvanceLastIndex())
	peer2I2 := util.MakeInstance(ballot1, logs[2].AdvanceLastIndex())
	logs[2].Append(peer2I1)
	logs[2].Append(peer2I2)

	logMap = peers[0].SendPrepares(leaderBallot)
	assert.EqualValues(t, 2, len(logMap))
	assert.EqualValues(t, ballot1, logMap[index1].GetBallot())
	assert.True(t, log.IsCommitted(logMap[index1]))
	assert.EqualValues(t, ballot1, logMap[index2].GetBallot())
	assert.True(t, log.IsInProgress(logMap[index2]))
}

func TestSendAccepts(t *testing.T) {
	setupPeers()
	defer tearDown()
	peers[0].StartServer()
	peers[0].Connect()

	ballot := peers[0].NextBallot()
	index1 := logs[0].AdvanceLastIndex()
	instance1 := util.MakeInstanceWithType(ballot, index1, pb.CommandType_PUT)

	r1 := peers[0].SendAccepts(ballot, index1, &pb.Command{Type: pb.CommandType_PUT}, 0)
	assert.EqualValues(t, Retry, r1.Type)
	assert.True(t, log.IsInProgress(logs[0].Find(index1)))
	assert.Nil(t, logs[1].Find(index1))
	assert.Nil(t, logs[2].Find(index1))

	peers[1].StartServer()
	peers[0].Connect()
	r2 := peers[0].SendAccepts(ballot, index1, &pb.Command{Type: pb.CommandType_PUT}, 0)
	assert.EqualValues(t, Ok, r2.Type)
	assert.True(t, log.IsCommitted(logs[0].Find(index1)))
	assert.True(t, log.IsEqualInstance(instance1, logs[1].Find(index1)))
	assert.Nil(t, logs[2].Find(index1))

	peers[2].StartServer()
	peers[0].Connect()
	index2 := logs[0].AdvanceLastIndex()
	instance2 := util.MakeInstanceWithType(ballot, index2, pb.CommandType_DEL)
	r3 := peers[0].SendAccepts(ballot, index2, &pb.Command{Type: pb.CommandType_DEL}, 0)
	time.Sleep(100 * time.Millisecond)
	assert.EqualValues(t, Ok, r3.Type)
	assert.True(t, log.IsCommitted(logs[0].Find(index2)))
	assert.True(t, log.IsEqualInstance(instance2, logs[1].Find(index2)))
	assert.True(t, log.IsEqualInstance(instance2, logs[2].Find(index2)))
}

func TestReplicateRetry(t *testing.T) {
	setupPeers()

	r1 := peers[0].Replicate(&pb.Command{}, 0)
	assert.Equal(t, Retry, r1.Type)
	assert.Equal(t, NoLeader, r1.Leader)

	peers[0].NextBallot()
	r2 := peers[0].Replicate(&pb.Command{}, 0)
	assert.Equal(t, Retry, r2.Type)
	assert.Equal(t, NoLeader, r2.Leader)
}

func TestReplicateSomeOneElseLeader(t *testing.T) {
	setupPeers()
	peers[0].StartServer()
	peers[1].StartServer()
	defer peers[0].Stop()
	defer peers[1].Stop()
	stub := makeStub(configs[0].Peers[0])

	ballot := peers[1].NextBallot()
	r1 := sendHeartbeat(stub, ballot, 0, 0)
	assert.EqualValues(t, pb.ResponseType_OK, r1.GetType())
	assert.EqualValues(t, 1, LeaderByPeer(peers[0]))

	r2 := peers[0].Replicate(&pb.Command{}, 0)
	assert.EqualValues(t, SomeElseLeader, r2.Type)
	assert.EqualValues(t, 1, r2.Leader)
}

func TestReplicateReady(t *testing.T) {
	setupPeers()
	defer tearDown()

	peers[1].StartServer()
	peers[2].StartServer()
	peers[0].Start()
	time.Sleep(1000 * time.Millisecond)
	result := peers[0].Replicate(&pb.Command{Type: pb.CommandType_PUT}, 0)
	assert.Equal(t, Ok, result.Type)
	assert.Equal(t, int64(0), result.Leader)
}

func TestOneLeaderElected(t *testing.T) {
	setupServer()
	defer tearDown()

	time.Sleep(1000 * time.Millisecond)
	assert.True(t, oneLeader())
}

func makeStub(target string) pb.MultiPaxosRPCClient {
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.
		NewCredentials()))
	if err != nil {
		panic("dial error")
	}
	stub := pb.NewMultiPaxosRPCClient(conn)
	return stub
}

func oneLeader() bool {
	if IsLeaderByPeer(peers[0]) {
		return !IsLeaderByPeer(peers[1]) && !IsLeaderByPeer(peers[2]) &&
			LeaderByPeer(peers[1]) == 0 && LeaderByPeer(peers[2]) == 0
	}
	if IsLeaderByPeer(peers[1]) {
		return !IsLeaderByPeer(peers[0]) && !IsLeaderByPeer(peers[2]) &&
			LeaderByPeer(peers[0]) == 1 && LeaderByPeer(peers[2]) == 1
	}
	if IsLeaderByPeer(peers[2]) {
		return !IsLeaderByPeer(peers[0]) && !IsLeaderByPeer(peers[1]) &&
			LeaderByPeer(peers[0]) == 2 && LeaderByPeer(peers[1]) == 2
	}
	return false
}

func LeaderByPeer(peer *Multipaxos) int64 {
	ballot, _ := peer.Ballot()
	return Leader(ballot)
}

func IsLeaderByPeer(peer *Multipaxos) bool {
	ballot, _ := peer.Ballot()
	return IsLeader(ballot, peer.Id())
}

func IsSomeoneElseLeaderByPeer(peer *Multipaxos) bool {
	return !IsLeaderByPeer(peer) && LeaderByPeer(peer) < MaxNumPeers
}

func sendHeartbeat(stub pb.MultiPaxosRPCClient, ballot int64,
	lastExecuted int64, globalLastExecuted int64) *pb.HeartbeatResponse {
	ctx := context.Background()
	request := pb.HeartbeatRequest{
		Ballot:             ballot,
		LastExecuted:       lastExecuted,
		GlobalLastExecuted: globalLastExecuted,
	}
	response, err := stub.Heartbeat(ctx, &request)
	if err != nil {
		return nil
	}
	return response
}

func sendPrepare(stub pb.MultiPaxosRPCClient, ballot int64) *pb.PrepareResponse {
	ctx := context.Background()
	request := pb.PrepareRequest{Ballot: ballot}
	response, err := stub.Prepare(ctx, &request)
	if err != nil {
		return nil
	}
	return response
}

func sendAccept(stub pb.MultiPaxosRPCClient,
	inst *pb.Instance) *pb.AcceptResponse {
	ctx := context.Background()
	request := pb.AcceptRequest{
		Instance: inst,
	}
	response, err := stub.Accept(ctx, &request)
	if err != nil {
		return nil
	}
	return response
}
