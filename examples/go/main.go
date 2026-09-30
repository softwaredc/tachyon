// Command go_example shows the two paths through tachyon from Go: the
// convenience Send/Receive pair, and the zero-copy Loan/Publish pair.
//
// Build and run:
//
//	export CGO_CFLAGS="-I/path/to/tachyon/include"
//	export CGO_LDFLAGS="-L/path/to/tachyon/build -ltachyon"
//	export LD_LIBRARY_PATH="/path/to/tachyon/build:$LD_LIBRARY_PATH"
//	go run main.go
//
// The child is started with os/exec so it is a genuinely separate process: it
// attaches to the region by name, exactly like a second service would.
package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/softwaredc/tachyon/bindings/go/tachyon"
)

const (
	regionName = "go_example"
	topicName  = "orders"
	msgs       = 20000
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "child" {
		if err := child(); err != nil {
			fmt.Fprintln(os.Stderr, "child:", err)
			os.Exit(1)
		}
		return
	}

	if err := parent(); err != nil {
		fmt.Fprintln(os.Stderr, "parent:", err)
		os.Exit(1)
	}
}

func parent() error {
	_ = tachyon.RemoveRegion(regionName)

	reg, err := tachyon.OpenRegion(regionName, &tachyon.RegionConf{
		Size:   64 << 20,
		Create: true,
	})
	if err != nil {
		return err
	}
	defer reg.Close()

	topic, err := reg.Topic(topicName, &tachyon.TopicConf{
		Partitions: 4,
		Capacity:   1024,
		PoolBytes:  16 << 20,
		Notify:     tachyon.NotifyHybrid,
	})
	if err != nil {
		return err
	}
	defer topic.Close()

	fmt.Printf("topic %q: %d partitions, %d slots each\n",
		topic.Name(), topic.Partitions(), topic.Capacity())

	// --- zero-copy: build the record directly in shared memory ------------
	// 16 bytes: order id (uint64) + user id (uint64)
	loan, err := topic.Loan(16)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(loan.Buf[0:8], 20261001)
	binary.LittleEndian.PutUint64(loan.Buf[8:16], 9911)
	if err := topic.Publish(loan, 16, 9911); err != nil {
		return err
	}
	fmt.Printf("published order 20261001 to partition %d\n", topic.Route(9911))

	// --- start the consumer ----------------------------------------------
	cmd := exec.Command(os.Args[0], "child")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	time.Sleep(200 * time.Millisecond)

	// --- bulk send --------------------------------------------------------
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = 'x'
	}

	start := time.Now()
	for i := 0; i < msgs; i++ {
		for {
			err := topic.Send(payload, uint64(i))
			if err == nil {
				break
			}
			if err == tachyon.ErrFull || err == tachyon.ErrNoMem {
				// Backpressure: the ring is full. In real code you would
				// either block, drop, or apply your own policy here.
				time.Sleep(time.Millisecond)
				continue
			}
			return err
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("sent %d messages in %v (%.0f msg/s)\n",
		msgs, elapsed, float64(msgs)/elapsed.Seconds())

	if err := cmd.Wait(); err != nil {
		return err
	}

	stats, err := topic.Stats()
	if err != nil {
		return err
	}
	fmt.Printf("stats: published=%d consumed=%d dropped=%d reclaimed=%d\n",
		stats.Published, stats.Consumed, stats.Dropped, stats.Reclaimed)

	return tachyon.RemoveRegion(regionName)
}

func child() error {
	reg, err := tachyon.OpenRegion(regionName, &tachyon.RegionConf{Create: false})
	if err != nil {
		return err
	}
	defer reg.Close()

	topic, err := reg.Topic(topicName, nil)
	if err != nil {
		return err
	}
	defer topic.Close()

	received := 0
	deadline := time.Now().Add(20 * time.Second)

	for received < msgs+1 { // +1 for the zero-copy order above
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %d messages", received)
		}

		// ReceiveAny scans partitions round-robin from a hint, so one
		// consumer can own the whole topic.
		msg, err := topic.ReceiveAny(1_000_000_000)
		if err == tachyon.ErrEmpty || err == tachyon.ErrTimeout {
			continue
		}
		if err != nil {
			return err
		}

		if received == 0 {
			orderID := binary.LittleEndian.Uint64(msg.Data[0:8])
			userID := binary.LittleEndian.Uint64(msg.Data[8:16])
			fmt.Printf("child: first message order=%d user=%d partition=%d\n",
				orderID, userID, msg.Partition)
		}
		received++
		if err := msg.Release(); err != nil {
			return err
		}
	}

	fmt.Printf("child: received %d messages\n", received)
	return nil
}
