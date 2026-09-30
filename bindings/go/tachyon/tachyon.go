// Package tachyon provides zero-copy inter-process communication over shared
// memory.
//
// Two processes on the same machine should not pay for two kernel copies to
// hand each other a message. tachyon puts the payload in shared memory once;
// only a 16-byte descriptor travels through the ring.
//
//	prod, _ := tachyon.OpenRegion("demo", &tachyon.RegionConf{
//	    Size: 64 << 20, Create: true,
//	})
//	topic, _ := prod.Topic("orders", &tachyon.TopicConf{
//	    Partitions: 8, Capacity: 1024, PoolBytes: 16 << 20,
//	})
//	topic.Send([]byte("hello"), 42)
//
//	// in another process
//	cons, _ := tachyon.OpenRegion("demo", &tachyon.RegionConf{Create: false})
//	topic, _ = cons.Topic("orders", nil)
//	msg, err := topic.Receive(topic.Route(42), -1)
//	if err == nil {
//	    fmt.Println(string(msg.Data))   // no copy: Data aliases shared memory
//	    msg.Release()
//	}
//
// Message ordering: messages sharing a routing key land in the same partition
// and are delivered in send order. Messages with different keys may be
// consumed concurrently with no ordering guarantee between them.
package tachyon

/*
#cgo LDFLAGS: -ltachyon
#include <stdlib.h>
#include <tachyon/tachyon.h>
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// Errors returned by this package. Wrap with errors.Is or compare directly.
type Err int

const (
	ErrOK Err = 0

	ErrInvalid  Err = -1
	ErrExist    Err = -2
	ErrNoEnt    Err = -3
	ErrNoMem    Err = -4
	ErrFull     Err = -5
	ErrEmpty    Err = -6
	ErrTimeout  Err = -7
	ErrTooBig   Err = -8
	ErrState    Err = -9
	ErrABI      Err = -10
	ErrIO       Err = -11
	ErrMap      Err = -12
	ErrBusy     Err = -13
	ErrInternal Err = -99
)

func (e Err) Error() string {
	if e == ErrOK {
		return "ok"
	}
	return fmt.Sprintf("tachyon: [%d] %s", int(e), C.GoString(C.ty_strerror(C.ty_err_t(e))))
}

// Notify policies.
const (
	NotifySpin   = C.TY_NOTIFY_SPIN
	NotifyFutex  = C.TY_NOTIFY_FUTEX
	NotifyHybrid = C.TY_NOTIFY_HYBRID
)

// The C library exposes opaque structs; cgo gives them real pointer types, so
// handles are typed rather than unsafe.Pointer. That keeps the compiler
// checking what we pass across the boundary.
type cRegion = *C.struct_ty_region
type cTopic = *C.struct_ty_topic

// MaxMsgSize is the largest payload that can be loaned in one message.
const MaxMsgSize = C.TACHYON_MAX_MSG_SIZE

// RegionConf mirrors ty_region_conf_t.
type RegionConf struct {
	Size        uint64
	Create      bool
	Exclusive   bool
	Hugepage    bool
	SpawnReaper bool
	LeaseNS     uint64
	Dir         string
}

func (c *RegionConf) toC() *C.ty_region_conf_t {
	conf := &C.ty_region_conf_t{}
	if c == nil {
		return conf
	}
	conf.size = C.size_t(c.Size)
	conf.create = boolToInt(c.Create)
	conf.exclusive = boolToInt(c.Exclusive)
	conf.hugepage = boolToInt(c.Hugepage)
	conf.spawn_reaper = boolToInt(c.SpawnReaper)
	conf.lease_ns = C.uint64_t(c.LeaseNS)
	if c.Dir != "" {
		conf.dir = C.CString(c.Dir)
	}
	return conf
}

func boolToInt(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

// Region is a mapped shared memory region.
type Region struct {
	handle cRegion
	name   string
}

// OpenRegion creates or attaches to a region by name.
func OpenRegion(name string, conf *RegionConf) (*Region, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	cconf := conf.toC()
	if conf != nil && conf.Dir != "" {
		defer C.free(unsafe.Pointer(cconf.dir))
	}

	var handle cRegion
	rc := Err(C.ty_region_open(cname, cconf, &handle))
	if rc != ErrOK {
		return nil, rc
	}
	return &Region{handle: handle, name: name}, nil
}

// Name returns the region's name.
func (r *Region) Name() string { return r.name }

// Size returns the mapped size in bytes.
func (r *Region) Size() uint64 {
	return uint64(C.ty_region_size(r.handle))
}

// Topic opens a topic, creating it if it does not exist. Concurrent creation
// from several processes is safe: the losers attach to the winner's topic.
func (r *Region) Topic(name string, conf *TopicConf) (*Topic, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	cconf := conf.toC()

	var handle cTopic
	rc := Err(C.ty_topic_create(r.handle, cname, cconf, &handle))
	if rc == ErrExist || rc == ErrNoEnt {
		rc = Err(C.ty_topic_open(r.handle, cname, &handle))
	}
	if rc != ErrOK {
		return nil, rc
	}
	return &Topic{handle: handle, name: name}, nil
}

// Heartbeat refreshes this process's liveness timestamp. Call it if your
// application blocks for long stretches outside tachyon.
func (r *Region) Heartbeat() { C.ty_heartbeat(r.handle) }

// Close detaches from the region. It does not delete it -- a producer must be
// able to publish and exit while its messages remain readable.
func (r *Region) Close() error {
	return check(C.ty_region_close(r.handle))
}

// RemoveRegion deletes a region by name.
func RemoveRegion(name string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	rc := Err(C.ty_region_remove(cname, nil))
	if rc == ErrNoEnt {
		return nil // idempotent, like rm -f
	}
	return check(C.ty_err_t(rc))
}

// TopicConf mirrors ty_topic_conf_t.
type TopicConf struct {
	Partitions uint32
	Capacity   uint32
	PoolBytes  uint64
	Notify     int32
}

func (c *TopicConf) toC() *C.ty_topic_conf_t {
	conf := &C.ty_topic_conf_t{}
	if c == nil {
		return conf
	}
	conf.partitions = C.uint32_t(c.Partitions)
	conf.capacity = C.uint32_t(c.Capacity)
	conf.pool_bytes = C.size_t(c.PoolBytes)
	conf.notify = C.ty_notify_mode_t(c.Notify)
	return conf
}

// Topic is a named channel inside a region.
type Topic struct {
	handle cTopic
	name   string
}

// Name returns the topic name.
func (t *Topic) Name() string { return t.name }

// Partitions returns the partition count.
func (t *Topic) Partitions() uint32 {
	return uint32(C.ty_topic_partitions(t.handle))
}

// Capacity returns the slot count per partition.
func (t *Topic) Capacity() uint32 {
	return uint32(C.ty_topic_capacity(t.handle))
}

// Route reports which partition a key maps to.
func (t *Topic) Route(key uint64) uint32 {
	return uint32(C.ty_route(t.handle, C.uint64_t(key)))
}

// Send publishes a payload under a routing key. One memcpy.
func (t *Topic) Send(payload []byte, key uint64) error {
	if len(payload) == 0 {
		return ErrInvalid
	}
	if len(payload) > MaxMsgSize {
		return ErrTooBig
	}
	rc := Err(C.ty_send(t.handle, unsafe.Pointer(&payload[0]),
		C.size_t(len(payload)), C.uint64_t(key)))
	return check(C.ty_err_t(rc))
}

// Loan borrows a writable buffer in shared memory. Write into Buf and then
// call Publish; this is the true zero-copy path with no memcpy at all.
func (t *Topic) Loan(size int) (*Loan, error) {
	var ptr unsafe.Pointer
	var cap C.size_t
	rc := Err(C.ty_loan(t.handle, C.size_t(size), &ptr, &cap))
	if e := check(C.ty_err_t(rc)); e != nil {
		return nil, e
	}
	return &Loan{
		topic: t,
		ptr:   ptr,
		Buf:   unsafe.Slice((*byte)(ptr), int(cap)),
	}, nil
}

// Loan is a borrowed, not-yet-published buffer.
type Loan struct {
	topic *Topic
	ptr   unsafe.Pointer
	Buf   []byte // aliases shared memory; writing here writes the message
}

// Publish commits a loaned buffer to key's partition.
func (t *Topic) Publish(l *Loan, length int, key uint64) error {
	rc := Err(C.ty_publish(t.handle, l.ptr, C.size_t(length), C.uint64_t(key)))
	return check(C.ty_err_t(rc))
}

// Abort returns a loan without publishing it.
func (l *Loan) Abort() error {
	return check(C.ty_abort(l.topic.handle, l.ptr))
}

// Message is a received message. Data aliases shared memory and is valid only
// until Release.
type Message struct {
	topic     *Topic
	raw       C.ty_msg_t
	Data      []byte
	Key       uint64
	Seq       uint64
	Partition uint32
	released  bool
}

// Receive waits for one message on a partition.
// timeoutNS < 0 blocks forever, 0 polls once, > 0 waits that long.
// Returns ErrEmpty or ErrTimeout when nothing arrives.
func (t *Topic) Receive(partition uint32, timeoutNS int64) (*Message, error) {
	var raw C.ty_msg_t
	rc := Err(C.ty_subscribe(t.handle, C.uint32_t(partition), &raw,
		C.int64_t(timeoutNS)))
	if rc != ErrOK {
		return nil, rc
	}
	return newMessage(t, raw), nil
}

// ReceiveAny takes a message from whichever partition has one waiting.
func (t *Topic) ReceiveAny(timeoutNS int64) (*Message, error) {
	var raw C.ty_msg_t
	var hint C.uint32_t
	rc := Err(C.ty_subscribe_any(t.handle, &hint, &raw, C.int64_t(timeoutNS)))
	if rc != ErrOK {
		return nil, rc
	}
	return newMessage(t, raw), nil
}

func newMessage(t *Topic, raw C.ty_msg_t) *Message {
	m := &Message{
		topic:     t,
		raw:       raw,
		Key:       uint64(raw.key),
		Seq:       uint64(raw.seq),
		Partition: uint32(raw.partition),
	}
	if raw.data != nil && raw.len > 0 {
		m.Data = unsafe.Slice((*byte)(raw.data), int(raw.len))
	}
	return m
}

// Release returns the chunk to the pool. Exactly once per message.
func (m *Message) Release() error {
	if m.released {
		return nil
	}
	m.released = true
	return check(C.ty_release(m.topic.handle, &m.raw))
}

// Depth reports pending messages on one partition, or across all with -1.
func (t *Topic) Depth(partition int32) (uint64, error) {
	var out C.uint64_t
	if e := check(C.ty_depth(t.handle, C.int(partition), &out)); e != nil {
		return 0, e
	}
	return uint64(out), nil
}

// Reclaim forces one lease sweep and returns how many chunks were recovered.
func (t *Topic) Reclaim() uint64 {
	return uint64(C.ty_reclaim_once(t.handle))
}

// Close detaches from the topic.
func (t *Topic) Close() error {
	return check(C.ty_topic_close(t.handle))
}

// Stats is a snapshot of the topic counters.
type Stats struct {
	Published   uint64
	Consumed    uint64
	Dropped     uint64
	LoanOK      uint64
	LoanNoMem   uint64
	Reclaimed   uint64
	WaitWakeups uint64
	SpinSpins   uint64
}

// Stats returns the current counters. A rising Reclaimed means something in
// your system is being killed; rising LoanNoMem means PoolBytes is too small.
func (t *Topic) Stats() (Stats, error) {
	var s C.ty_stats_t
	if e := check(C.ty_topic_stats(t.handle, &s)); e != nil {
		return Stats{}, e
	}
	return Stats{
		Published:   uint64(s.published),
		Consumed:    uint64(s.consumed),
		Dropped:     uint64(s.dropped),
		LoanOK:      uint64(s.loan_ok),
		LoanNoMem:   uint64(s.loan_nomem),
		Reclaimed:   uint64(s.reclaimed),
		WaitWakeups: uint64(s.wait_wakeups),
		SpinSpins:   uint64(s.spin_spins),
	}, nil
}

// NowNS returns a monotonic timestamp in nanoseconds.
func NowNS() uint64 { return uint64(C.ty_now_ns()) }

func check(rc C.ty_err_t) error {
	if rc == 0 {
		return nil
	}
	return Err(rc)
}
