// Bryan SebaRaj
// Raft tests for CPSC 426 - Lab 3

package raft

import (
	"fmt"
	// "go/format"
	"math/rand"
	"runtime"
	// "sync"
	// "sync/atomic"
	"testing"
	"time"

	"6.824/labrpc"
)

const RaftTestTimeout = 1000 * time.Millisecond

// leader election - checks that a leader is not elected from the initializations of the nodes (different that TestReElection3A)
func TestNoElectionWithoutQuorumThenElectionWithQuorum(t *testing.T) {
	servers := 5
	// cfg := make_config(t, servers, false, false)
	// copied from make_config
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = servers
	cfg.applyErr = make([]string, cfg.n)
	cfg.rafts = make([]*Raft, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.saved = make([]*Persister, cfg.n)
	cfg.endnames = make([][]string, cfg.n)
	cfg.logs = make([]map[int]interface{}, cfg.n)
	cfg.start = time.Now()

	cfg.setunreliable(false)

	cfg.net.LongDelays(true)

	applier := cfg.applier
	// create a full set of Rafts.
	for i := 0; i < cfg.n; i++ {
		cfg.logs[i] = map[int]interface{}{}
		cfg.start1(i, applier)
	}

	// connect minority of servers; not enough for majority/leader election
	for i := 0; i < int(cfg.n/2); i++ {
		cfg.connect(i)
	}

	defer cfg.cleanup()

	cfg.begin("Test: No election if only minorty of peers are connected")
	// checks that no leader is elected
	cfg.checkNoLeader()
	for i := int(cfg.n / 2); i < cfg.n; i++ {
		cfg.connect(i)
	}

	newElectedLeader := cfg.checkOneLeader()
	// checks that a leader is elected
	if newElectedLeader == -1 {
		t.Fatalf("expected a leader to be elected, but no leader was elected")
	}
	cfg.end()
}

// leader election - elect leader, then disconnect non-leader nodes, allowing them to become candidates in higher terms
func TestDisconnectedCandidateReconnect(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()
	cfg.begin("Test: Disconnect non-leader nodes, allow them to become candidates in higher terms")
	leader := cfg.checkOneLeader()
	term1 := cfg.checkTerms()
	// println("leader: ", leader)
	for i := 0; i < servers; i++ {
		if i != leader {
			cfg.disconnect(i)
		}
	}
	// give time for disconnected follower nodes to timeout and become candidated
	time.Sleep(3 * time.Second)
	// only connect one of the disconnected nodes
	for i := 0; i < servers; i++ {
		if i != leader {
			cfg.connect(i)
		}
	}
	// that node may now become the leader, but the leader will deterministically have a higher term
	time.Sleep(3 * time.Second)
	cfg.checkOneLeader()
	term2 := cfg.checkTerms()
	// println("new leader: ", newLeader)
	if term2 <= term1 {
		t.Fatalf("expected new term to be greater than old term, but new term is not greater")
	}

	cfg.end()
}

// unreliable leader election

func TestUnreliableElection(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true, false)
	defer cfg.cleanup()
	cfg.begin("Test: Unreliable leader election")
	time.Sleep(RaftElectionTimeout)
	cfg.checkOneLeader()

	loop := 5
	var leader int
	var currentTerm, newTerm int
	for i := 1; i < loop; i++ {
		// disconnect three nodes
		leader = cfg.checkOneLeader()
		currentTerm = cfg.checkTerms()
		i1 := leader % servers
		i2 := (leader + 1) % servers
		i3 := (leader + 2) % servers
		cfg.disconnect(i1)
		cfg.disconnect(i2)
		cfg.disconnect(i3)
		time.Sleep(2 * time.Second)
		cfg.connect(i1)
		cfg.connect(i2)
		cfg.connect(i3)
		time.Sleep(RaftElectionTimeout)

		// check that a leader is elected in a new term
		cfg.checkOneLeader()
		newTerm = cfg.checkTerms()
		if newTerm <= currentTerm {
			t.Fatalf("expected new term to be greater than old term, but new term is not greater")
		}
	}
	cfg.end()
}

// log replication -
func TestReplicationImpactOnLeaderElection(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test: Log replication custom")
	cfg.disconnect(0)
	// sleep in of possible leader election
	time.Sleep(RaftElectionTimeout)
	cfg.one(101, 2, true)
	cfg.one(102, 2, true)

	// give follower node time to commit to its own log
	time.Sleep(2 * time.Second)

	// disconnect leader
	leader := cfg.checkOneLeader()
	cfg.disconnect(leader)
	cfg.connect(0)

	// leader election should occur, if 0 wins, logs should be empty, if 1 wins, logs should be same

	time.Sleep(2 * time.Second)
	newLeader := cfg.checkOneLeader()

	time.Sleep(1 * time.Second)

	if newLeader == 0 {
		for i := 1; i <= 2; i++ {
			nd, _ := cfg.nCommitted(i)
			if nd > 0 {
				t.Fatalf("cmd %v committed", i)
			}
		}
	} else {
		// check that logs are the same
		for i := 1; i <= 2; i++ {
			nd, _ := cfg.nCommitted(i)
			if nd < 2 {
				t.Fatalf("cmd %v not committed", i)
			}
		}
	}

	cfg.end()
}

// persistence -
func TestReplicationAndPersistenceOnUnreliableNetwork(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true, false)
	defer cfg.cleanup()

	cfg.begin("Test: Persistence on unreliable network")
	// println("1")
	// for i := 0; i < 105; i++ {
	// 	for j := 0; j < servers; j++ {
	// 		print(cfg.logs[j][i], " ")
	// 	}
	// 	println()
	// }
	cfg.one(101, 5, true)
	// println("2")
	// for i := 0; i < 105; i++ {
	// 	for j := 0; j < servers; j++ {
	// 		print(cfg.logs[j][i], " ")
	// 	}
	// 	println()
	// }
	time.Sleep(2 * time.Second)
	cfg.checkOneLeader()
	for i := 0; i < servers; i++ {
		cfg.crash1(i)
	}
	// println("crashed")
	// time.Sleep(1 * time.Second)
	for i := 0; i < servers; i++ {
		cfg.start1(i, cfg.applier)
		cfg.connect(i)
	}
	// println("3")
	// for i := 0; i < 105; i++ {
	// 	for j := 0; j < servers; j++ {
	// 		print(cfg.logs[j][i], " ")
	// 	}
	// 	println()
	// }
	time.Sleep(2 * time.Second)
	cfg.checkOneLeader()
	cfg.one(102, 5, true) // will only not error out when committed on at least one node
	cfg.one(103, 5, true)

	time.Sleep(2 * time.Second)

	// compare logs of all nodes

	for i := 0; i < servers; i++ {
		cfg.start1(i, cfg.applier)
		cfg.connect(i)
	}
	time.Sleep(1 * time.Second)
	// println("4")
	// // testing this, everyting above works
	// for i := 0; i < 105; i++ {
	// 	for j := 0; j < servers; j++ {
	// 		print(cfg.logs[j][i], " ")
	// 	}
	// 	println()
	// }
	//
	// should be committed on all by now
	for i := 1; i <= 3; i++ {
		nd, _ := cfg.nCommitted(i)
		if nd < servers {
			t.Fatalf("cmd %v not committed", i)
		}
	}

	cfg.end()
}
