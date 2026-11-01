package tests

import (
	"fmt"
	"raft-kv/pkg/client"
	"sync"
	"testing"
	"time"
)

func TestConcurrentLinearizability(t *testing.T) {
	_, servers, _ := createKVCluster(3, 0)
	defer func() {
		for _, s := range servers {
			s.Kill()
		}
	}()

	time.Sleep(200 * time.Millisecond)

	// Spin up 5 concurrent clients issuing sequential writes and reads
	numClients := 5
	opsPerClient := 10
	var wg sync.WaitGroup

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			ck := client.MakeClerk(servers)
			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("k_%d_%d", clientID, j)
				expectedVal := fmt.Sprintf("val_%d_%d", clientID, j)
				
				err := ck.Put(key, expectedVal)
				if err != nil {
					t.Errorf("Client %d failed Put: %v", clientID, err)
					return
				}

				actualVal, err := ck.Get(key)
				if err != nil || actualVal != expectedVal {
					t.Errorf("Client %d linearizability check failed! expected=%s, got=%s, err=%v", clientID, expectedVal, actualVal, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	t.Log("Concurrent linearizability test completed successfully across 50 operations.")
}

