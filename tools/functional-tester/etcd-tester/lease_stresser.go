// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// time to live for lease
const TTL = 30

type leaseStressConfig struct {
	numLeases    int
	keysPerLease int
}

type leaseStresser struct {
	endpoint string
	cancel   func()
	conn     *grpc.ClientConn
	kvc      pb.KVClient
	lc       pb.LeaseClient
	ctx      context.Context

	success      int
	failure      int
	numLeases    int
	keysPerLease int

	// mu Mutex is used to protect the shared maps aliveLeases and revokedLeases
	// which are accessed and modified by different go routines.
	mu            sync.Mutex
	aliveLeases   map[int64]struct{}
	revokedLeases map[int64]struct{}

	runWg   sync.WaitGroup
	aliveWg sync.WaitGroup
}

type leaseStresserBuilder func(m *member) Stresser

func newLeaseStresserBuilder(lsConfig *leaseStressConfig, lsTracker func(*leaseStresser)) leaseStresserBuilder {
	return func(mem *member) Stresser {
		ls := &leaseStresser{
			endpoint:     mem.grpcAddr(),
			numLeases:    lsConfig.numLeases,
			keysPerLease: lsConfig.keysPerLease,
		}
		lsTracker(ls)
		return ls
	}
}

func (ls *leaseStresser) setupOnce() error {
	if ls.aliveLeases != nil {
		return nil
	}
	if ls.numLeases == 0 {
		panic("expect numLeases to be set")
	}
	if ls.keysPerLease == 0 {
		panic("expect keysPerLease to be set")
	}

	conn, err := grpc.Dial(ls.endpoint, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("%v (%s)", err, ls.endpoint)
	}
	ls.conn = conn
	ls.kvc = pb.NewKVClient(conn)
	ls.lc = pb.NewLeaseClient(conn)

	ls.aliveLeases = make(map[int64]struct{})
	ls.revokedLeases = make(map[int64]struct{})
	return nil
}

func (ls *leaseStresser) Stress() error {
	plog.Infof("lease Stresser %v starting ...", ls.endpoint)
	if err := ls.setupOnce(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	ls.cancel = cancel
	ls.ctx = ctx

	ls.runWg.Add(1)
	go ls.run()
	return nil
}

func (ls *leaseStresser) run() {
	defer ls.runWg.Done()
	ls.restartKeepAlives()
	for ls.ctx.Err() == nil {
		plog.Debugf("creating lease on %v ", ls.endpoint)
		ls.createLeases()
		plog.Debugf("done creating lease on %v ", ls.endpoint)
		plog.Debugf("dropping lease on %v ", ls.endpoint)
		ls.randomlyDropLeases()
		plog.Debugf("done dropping lease on %v ", ls.endpoint)
	}
}

func (ls *leaseStresser) restartKeepAlives() {
	for leaseID := range ls.aliveLeases {
		ls.aliveWg.Add(1)
		go func(id int64) {
			ls.keepLeaseAlive(id)
		}(leaseID)
	}
}

func (ls *leaseStresser) createLeases() {
	neededLeases := ls.numLeases - len(ls.aliveLeases)
	var wg sync.WaitGroup
	wg.Add(neededLeases)
	for i := 0; i < neededLeases; i++ {
		go func() {
			defer wg.Done()
			leaseID, err := ls.createLeaseAndKeepAlive()
			if err != nil {
				plog.Errorf("lease creation error: (%v)", err)
				return
			}
			plog.Debugf("lease %v created ", leaseID)
			// if attaching keys to the lease encountered an error, we don't add the lease to the aliveLeases map
			// because invariant check on the lease will fail due to keys not found
			if err := ls.attachKeysWithLease(leaseID); err != nil {
				return
			}
			ls.atomicAdd(leaseID, ls.aliveLeases)
		}()
	}
	wg.Wait()
}

func (ls *leaseStresser) atomicAdd(leaseID int64, leases map[int64]struct{}) {
	ls.mu.Lock()
	leases[leaseID] = struct{}{}
	ls.mu.Unlock()
}

func (ls *leaseStresser) atomicRemove(leaseID int64, leases map[int64]struct{}) {
	ls.mu.Lock()
	delete(leases, leaseID)
	ls.mu.Unlock()
}

func (ls *leaseStresser) randomlyDropLeases() {
	var wg sync.WaitGroup
	for l := range ls.aliveLeases {
		wg.Add(1)
		go func(leaseID int64) {
			defer wg.Done()
			dropped, err := ls.randomlyDropLease(leaseID)
			// if randomlyDropLease encountered an error such as context is cancelled, remove the lease from aliveLeases
			// since we don't really know whether the lease is dropped or not.
			if err != nil {
				ls.atomicRemove(leaseID, ls.aliveLeases)
				return
			}
			if !dropped {
				return
			}
			plog.Debugf("lease %v dropped ", leaseID)
			ls.atomicAdd(leaseID, ls.revokedLeases)
			ls.atomicRemove(leaseID, ls.aliveLeases)
		}(l)
	}
	wg.Wait()
}

func (ls *leaseStresser) getLeaseByID(ctx context.Context, leaseID int64) (*pb.LeaseTimeToLiveResponse, error) {
	ltl := &pb.LeaseTimeToLiveRequest{
		ID:   leaseID,
		Keys: true,
	}

	return ls.lc.LeaseTimeToLive(ctx, ltl, grpc.FailFast(false))
}

func (ls *leaseStresser) hasLeaseExpired(ctx context.Context, leaseID int64) (bool, error) {
	resp, err := ls.getLeaseByID(ctx, leaseID)
	plog.Debugf("hasLeaseExpired %v resp %v error (%v)", leaseID, resp, err)
	if rpctypes.Error(err) == rpctypes.ErrLeaseNotFound {
		return true, nil
	}
	return false, err
}

// The keys attached to the lease has the format of "<leaseID>_<idx>" where idx is the ordering key creation
// Since the format of keys contains about leaseID, finding keys base on "<leaseID>" prefix
// determines whether the attached keys for a given leaseID has been deleted or not
func (ls *leaseStresser) hasKeysAttachedToLeaseExpired(ctx context.Context, leaseID int64) (bool, error) {
	// plog.Infof("retriving keys attached to lease %v", leaseID)
	resp, err := ls.kvc.Range(ctx, &pb.RangeRequest{
		Key:      []byte(fmt.Sprintf("%d", leaseID)),
		RangeEnd: []byte(clientv3.GetPrefixRangeEnd(fmt.Sprintf("%d", leaseID))),
	}, grpc.FailFast(false))
	plog.Debugf("hasKeysAttachedToLeaseExpired %v resp %v error (%v)", leaseID, resp, err)
	if err != nil {
		plog.Errorf("retriving keys attached to lease %v error: (%v)", leaseID, err)
		return false, err
	}
	return len(resp.Kvs) == 0, nil
}

func (ls *leaseStresser) createLeaseAndKeepAlive() (int64, error) {
	leaseID, err := ls.createLease()
	if err != nil {
		return -1, err
	}
	// keep track of all the keep lease alive go routines
	ls.aliveWg.Add(1)
	go ls.keepLeaseAlive(leaseID)
	return leaseID, nil
}

func (ls *leaseStresser) createLease() (int64, error) {
	resp, err := ls.lc.LeaseGrant(ls.ctx, &pb.LeaseGrantRequest{
		TTL: TTL,
	})
	if err != nil {
		return -1, err
	}
	return resp.ID, nil
}

func (ls *leaseStresser) keepLeaseAlive(leaseID int64) {
	defer ls.aliveWg.Done()
	ctx, cancel := context.WithCancel(ls.ctx)
	stream, err := ls.lc.LeaseKeepAlive(ctx, grpc.FailFast(false))
	for {
		select {
		case <-time.After(time.Second):
		case <-ls.ctx.Done():
			plog.Debugf("keepLeaseAlive lease %v context canceled ", leaseID)
			return
		}
		if err != nil {
			plog.Debugf("keepLeaseAlive lease %v creates stream error: (%v)", leaseID, err)
			cancel()
			ctx, cancel = context.WithCancel(ls.ctx)
			stream, err = ls.lc.LeaseKeepAlive(ctx, grpc.FailFast(false))
			continue
		}
		err = stream.Send(&pb.LeaseKeepAliveRequest{ID: leaseID})
		if err != nil {
			plog.Debugf("keepLeaseAlive stream sends lease %v error (%v) ", leaseID, err)
			continue
		}
		respRC, err := stream.Recv()
		if err != nil {
			plog.Debugf("keepLeaseAlive stream receives lease %v stream error (%v) ", leaseID, err)
			continue
		}
		// lease expires after TTL become 0
		// don't send keepalive if the lease has expired
		if respRC.TTL <= 0 {
			plog.Debugf("keepLeaseAlive stream receives lease %v has TTL <= 0 ", leaseID)
			return
		}
	}
}

// attachKeysWithLease function attaches keys to the lease.
// the format of key is the concat of leaseID + '_' + '<order of key creation>'
// e.g 5186835655248304152_0 for first created key and 5186835655248304152_1 for second created key
func (ls *leaseStresser) attachKeysWithLease(leaseID int64) error {
	var txnPuts []*pb.RequestOp
	for j := 0; j < ls.keysPerLease; j++ {
		txnput := &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte(fmt.Sprintf("%d%s%d", leaseID, "_", j)),
			Value: []byte(fmt.Sprintf("bar")), Lease: leaseID}}}
		txnPuts = append(txnPuts, txnput)
	}
	// keep retrying until lease is not found or ctx is being canceled
	for ls.ctx.Err() == nil {
		txn := &pb.TxnRequest{Success: txnPuts}
		_, err := ls.kvc.Txn(ls.ctx, txn)
		if err == nil {
			return nil
		}
		if rpctypes.Error(err) == rpctypes.ErrLeaseNotFound {
			return err
		}
	}

	return ls.ctx.Err()
}

// randomlyDropLease drops the lease only when the rand.Int(2) returns 1.
// This creates a 50/50 percents chance of dropping a lease
func (ls *leaseStresser) randomlyDropLease(leaseID int64) (bool, error) {
	if rand.Intn(2) != 0 {
		return false, nil
	}
	// keep retrying until a lease is dropped or ctx is being canceled
	for ls.ctx.Err() == nil {
		_, err := ls.lc.LeaseRevoke(ls.ctx, &pb.LeaseRevokeRequest{ID: leaseID})
		if err == nil || rpctypes.Error(err) == rpctypes.ErrLeaseNotFound {
			plog.Debugf("lease %v dropped", leaseID)
			return true, nil
		}
	}
	plog.Debugf("randomlyDropLease error: (%v)", ls.ctx.Err())
	return false, ls.ctx.Err()
}

func (ls *leaseStresser) Cancel() {
	plog.Debugf("lease stresser %q is canceling...", ls.endpoint)
	ls.cancel()
	ls.runWg.Wait()
	ls.aliveWg.Wait()
	plog.Infof("lease stresser %q is canceled", ls.endpoint)
}

func (ls *leaseStresser) Report() (int, int) {
	return ls.success, ls.failure
}
