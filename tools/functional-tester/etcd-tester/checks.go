// Copyright 2016 The etcd Authors
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
	"time"

	"golang.org/x/net/context"
)

type Checker interface {
	// Check returns an error if the system fails a consistency check.
	Check() error
}

type hashChecker struct {
	tt *tester
}

func newHashChecker(tt *tester) Checker { return &hashChecker{tt} }

func (hc *hashChecker) Check() (err error) {
	plog.Printf("%s fetching current revisions...", hc.tt.logPrefix())
	var (
		revs   map[string]int64
		hashes map[string]int64
		ok     bool
	)
	for i := 0; i < 7; i++ {
		time.Sleep(time.Second)

		revs, hashes, err = hc.tt.cluster.getRevisionHash()
		if err != nil {
			plog.Printf("%s #%d failed to get current revisions (%v)", hc.tt.logPrefix(), i, err)
			continue
		}
		if _, ok = getSameValue(revs); ok {
			break
		}

		plog.Printf("%s #%d inconsistent current revisions %+v", hc.tt.logPrefix(), i, revs)
	}
	if !ok || err != nil {
		return fmt.Errorf("checking current revisions failed [err: %v, revisions: %v]", err, revs)
	}
	plog.Printf("%s all members are consistent with current revisions [revisions: %v]", hc.tt.logPrefix(), revs)

	plog.Printf("%s checking current storage hashes...", hc.tt.logPrefix())
	if _, ok = getSameValue(hashes); !ok {
		return fmt.Errorf("inconsistent hashes [%v]", hashes)
	}

	plog.Printf("%s all members are consistent with storage hashes", hc.tt.logPrefix())
	return nil
}

type leaseChecker struct {
	leaseStressers []Stresser
}

func newLeaseChecker(leaseStressers []Stresser) Checker { return &leaseChecker{leaseStressers} }

func (lc *leaseChecker) Check() error {
	plog.Info("lease stresser invariant check...")
	errc := make(chan error)
	for _, ls := range lc.leaseStressers {
		go func(s Stresser) { errc <- lc.invariantCheck(s) }(ls)
	}
	var errs []error
	for i := 0; i < len(lc.leaseStressers); i++ {
		if err := <-errc; err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("lease stresser encounters error: (%v)", errs[0])
}

func (lc *leaseChecker) invariantCheck(lStresser Stresser) error {
	ls := lStresser.(*leaseStresser)
	// check revoked leases
	plog.Debugf("revoked leases %v", ls.revokedLeases)
	if err := leasesCheck(true, ls, ls.revokedLeases); err != nil {
		return err
	}

	ls.revokedLeases = make(map[int64]struct{})

	// check alive leases
	plog.Debugf("alive leases %v", ls.aliveLeases)
	if err := leasesCheck(false, ls, ls.aliveLeases); err != nil {
		return err
	}

	return nil
}

func leasesCheck(expired bool, ls *leaseStresser, leases map[int64]struct{}) error {
	// timed out after 10 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for leaseID := range leases {
		keysExpired, err := ls.hasKeysAttachedToLeaseExpired(ctx, leaseID)
		if err != nil {
			plog.Errorf("hasKeysAttachedToLeaseExpired error: (%v)", err)
			return err
		}
		leaseExpired, err := ls.hasLeaseExpired(ctx, leaseID)
		if err != nil {
			plog.Errorf("hasLeaseExpired error: (%v)", err)
			return err
		}
		if leaseExpired != keysExpired {
			return fmt.Errorf("lease %v expiration mismatch (lease expired=%v, keys expired=%v)", leaseID, leaseExpired, keysExpired)
		}
		if leaseExpired != expired {
			return fmt.Errorf("lease %v expected expired=%v, got %v", leaseID, expired, leaseExpired)
		}
	}
	return nil
}

type compositeChecker struct {
	checkers []Checker
}

func newCompositeChecker(checkers []Checker) Checker {
	return &compositeChecker{checkers}
}

func (cchecker *compositeChecker) Check() error {
	for _, checker := range cchecker.checkers {
		if err := checker.Check(); err != nil {
			return err
		}
	}

	return nil
}

type noChecker struct{}

func newNoChecker() Checker        { return &noChecker{} }
func (nc *noChecker) Check() error { return nil }
