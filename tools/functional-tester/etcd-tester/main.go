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
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/coreos/pkg/capnslog"
	"github.com/prometheus/client_golang/prometheus"
)

var plog = capnslog.NewPackageLogger("github.com/coreos/etcd", "etcd-tester")

const (
	defaultClientPort    = 2379
	defaultPeerPort      = 2380
	defaultFailpointPort = 2381
)

func main() {
	endpointStr := flag.String("agent-endpoints", "localhost:9027", "HTTP RPC endpoints of agents. Do not specify the schema.")
	clientPorts := flag.String("client-ports", "", "etcd client port for each agent endpoint")
	peerPorts := flag.String("peer-ports", "", "etcd peer port for each agent endpoint")
	failpointPorts := flag.String("failpoint-ports", "", "etcd failpoint port for each agent endpoint")

	datadir := flag.String("data-dir", "agent.etcd", "etcd data directory location on agent machine.")
	stressKeyLargeSize := flag.Uint("stress-key-large-size", 32*1024+1, "the size of each large key written into etcd.")
	stressKeySize := flag.Uint("stress-key-size", 100, "the size of each small key written into etcd.")
	stressKeySuffixRange := flag.Uint("stress-key-count", 250000, "the count of key range written into etcd.")
	limit := flag.Int("limit", -1, "the limit of rounds to run failure set (-1 to run without limits).")
	stressQPS := flag.Int("stress-qps", 10000, "maximum number of stresser requests per second.")
	schedCases := flag.String("schedule-cases", "", "test case schedule")
	consistencyCheck := flag.Bool("consistency-check", true, "true to check consistency (revision, hash)")
	isV2Only := flag.Bool("v2-only", false, "'true' to run V2 only tester.")
	stresserType := flag.String("stresser", "default", "specify stresser (\"default\" or \"nop\").")
	failureTypes := flag.String("failures", "default,failpoints", "specify failures (concat of \"default\" and \"failpoints\").")
	flag.Parse()

	eps := strings.Split(*endpointStr, ",")
	cports := portsFromArg(*clientPorts, len(eps), defaultClientPort)
	pports := portsFromArg(*peerPorts, len(eps), defaultPeerPort)
	fports := portsFromArg(*failpointPorts, len(eps), defaultFailpointPort)
	agents := make([]agentConfig, len(eps))

	for i := range eps {
		agents[i].endpoint = eps[i]
		agents[i].clientPort = cports[i]
		agents[i].peerPort = pports[i]
		agents[i].failpointPort = fports[i]
		agents[i].datadir = *datadir
	}

	sConfig := &stressConfig{
		qps:            *stressQPS,
		keyLargeSize:   int(*stressKeyLargeSize),
		keySize:        int(*stressKeySize),
		keySuffixRange: int(*stressKeySuffixRange),
		v2:             *isV2Only,
	}

	lsConfig := &leaseStressConfig{
		numLeases:    10,
		keysPerLease: 10,
	}
	// leaseStressers keep tracks of all the initialized leaseStressers. It is being used by leaseChecker for invariant check
	var leaseStressers []Stresser
	// pass f to the lease stresser builder; call f(ls) when building a new lease stresser
	f := func(ls *leaseStresser) { leaseStressers = append(leaseStressers, ls) }

	c := &cluster{
		agents:               agents,
		v2Only:               *isV2Only,
		stressBuilder:        newStressBuilder(*stresserType, sConfig),
		leaseStresserBuilder: newLeaseStresserBuilder(lsConfig, f),
	}

	if err := c.bootstrap(); err != nil {
		plog.Fatal(err)
	}
	defer c.Terminate()

	// ensure cluster is fully booted to know failpoints are available
	c.WaitHealth()

	var failures []failure

	if failureTypes != nil && *failureTypes != "" {
		failures = makeFailures(*failureTypes, c)
	}

	if len(failures) == 0 {
		plog.Infof("no failures\n")
		failures = append(failures, newFailureNop())
	}

	schedule := failures
	if schedCases != nil && *schedCases != "" {
		cases := strings.Split(*schedCases, " ")
		schedule = make([]failure, len(cases))
		for i := range cases {
			caseNum := 0
			n, err := fmt.Sscanf(cases[i], "%d", &caseNum)
			if n == 0 || err != nil {
				plog.Fatalf(`couldn't parse case "%s" (%v)`, cases[i], err)
			}
			schedule[i] = failures[caseNum]
		}
	}
	t := &tester{
		failures: schedule,
		cluster:  c,
		limit:    *limit,
	}

	checkers := make([]Checker, 0)

	if *consistencyCheck && !c.v2Only {
		checkers = append(checkers, newHashChecker(t))
	} else {
		checkers = append(checkers, newNoChecker())
	}

	lc := newLeaseChecker(leaseStressers)

	checkers = append(checkers, lc)

	t.checker = newCompositeChecker(checkers)

	sh := statusHandler{status: &t.status}
	http.Handle("/status", sh)
	http.Handle("/metrics", prometheus.Handler())
	go func() { plog.Fatal(http.ListenAndServe(":9028", nil)) }()

	t.runLoop()
}

// portsFromArg converts a comma separated list into a slice of ints
func portsFromArg(arg string, n, defaultPort int) []int {
	ret := make([]int, n)
	if len(arg) == 0 {
		for i := range ret {
			ret[i] = defaultPort
		}
		return ret
	}
	s := strings.Split(arg, ",")
	if len(s) != n {
		fmt.Printf("expected %d ports, got %d (%s)\n", n, len(s), arg)
		os.Exit(1)
	}
	for i := range s {
		if _, err := fmt.Sscanf(s[i], "%d", &ret[i]); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	return ret
}

func makeFailures(types string, c *cluster) []failure {
	var failures []failure

	fails := strings.Split(types, ",")
	for i := range fails {
		switch fails[i] {
		case "default":
			defaultFailures := []failure{
				newFailureKillAll(),
				newFailureKillMajority(),
				newFailureKillOne(),
				newFailureKillLeader(),
				newFailureKillOneForLongTime(),
				newFailureKillLeaderForLongTime(),
				newFailureIsolate(),
				newFailureIsolateAll(),
				newFailureSlowNetworkOneMember(),
				newFailureSlowNetworkLeader(),
				newFailureSlowNetworkAll(),
			}
			failures = append(failures, defaultFailures...)

		case "failpoints":
			fpFailures, fperr := failpointFailures(c)
			if len(fpFailures) == 0 {
				plog.Infof("no failpoints found (%v)", fperr)
			}
			failures = append(failures, fpFailures...)

		default:
			plog.Errorf("unknown failure: %s\n", fails[i])
			os.Exit(1)
		}
	}

	return failures
}
