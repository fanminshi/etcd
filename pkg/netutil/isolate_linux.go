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

package netutil

import (
	"fmt"
	"os/exec"
)

// DropPort drops all tcp packets that are received from the given port and sent to the given port.
func DropPort(port int) error {
	cmdStr := fmt.Sprintf("sudo iptables -A OUTPUT -p tcp --destination-port %d -j DROP", port)
	if _, err := exec.Command("/bin/sh", "-c", cmdStr).Output(); err != nil {
		return err
	}
	cmdStr = fmt.Sprintf("sudo iptables -A INPUT -p tcp --destination-port %d -j DROP", port)
	_, err := exec.Command("/bin/sh", "-c", cmdStr).Output()
	return err
}

// RecoverPort stops dropping tcp packets at given port.
func RecoverPort(port int) error {
	cmdStr := fmt.Sprintf("sudo iptables -D OUTPUT -p tcp --destination-port %d -j DROP", port)
	if _, err := exec.Command("/bin/sh", "-c", cmdStr).Output(); err != nil {
		return err
	}
	cmdStr = fmt.Sprintf("sudo iptables -D INPUT -p tcp --destination-port %d -j DROP", port)
	_, err := exec.Command("/bin/sh", "-c", cmdStr).Output()
	return err
}

// SetLatency adds latency in millisecond scale with random variations.
func SetLatency(ms, rv int) error {
	fmt.Printf("SetLatency ms %v rv %v\n", ms, rv)
	ifce, err := GetDefaultInterface()
	fmt.Printf("SetLatency GetDefaultInterface() status: ifce %v, err (%v)\n", ifce, err)
	if err != nil {
		return err
	}

	if rv > ms {
		rv = 1
	}
	cmdStr := fmt.Sprintf("sudo tc qdisc add dev %s root netem delay %dms %dms distribution normal", ifce, ms, rv)
	fmt.Printf("SetLatency cmd: %v\n", cmdStr)
	_, err = exec.Command("/bin/sh", "-c", cmdStr).Output()
	if err != nil {
		fmt.Printf("SetLatency cmd: %v error (%v)\n", cmdStr, err)
		// the rule has already been added. Overwrite it.
		cmdStr = fmt.Sprintf("sudo tc qdisc change dev %s root netem delay %dms %dms distribution normal", ifce, ms, rv)
		fmt.Printf("SetLatency cmd: %v\n", cmdStr)
		_, err = exec.Command("/bin/sh", "-c", cmdStr).Output()
		if err != nil {
			fmt.Printf("SetLatency cmd: %v error (%v)\n", cmdStr, err)
			return err
		}
	}
	fmt.Printf("SetLatency ms %v rv %v done\n", ms, rv)

	return nil
}

// RemoveLatency resets latency configurations.
func RemoveLatency() error {
	fmt.Printf("RemoveLatency\n")
	ifce, err := GetDefaultInterface()
	fmt.Printf("RemoveLatency GetDefaultInterface() status: ifce %v, err (%v)\n", ifce, err)
	if err != nil {
		return err
	}
	cmdStr := fmt.Sprintf("sudo tc qdisc del dev %s root netem", ifce)
	fmt.Printf("RemoveLatency cmd: %v\n", cmdStr)
	_, err = exec.Command("/bin/sh", "-c", cmdStr).Output()
	if err != nil {
		fmt.Printf("RemoveLatency cmd: %v\n error (%v)\n", cmdStr, err)
	}
	return err
}
