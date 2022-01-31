package status

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/xujiajun/nutsdb"
)

func ExampleNutsDBStatus_newRecord() {

	// Ensure tmp path is clean
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_newRecord")

	status, err := NewNutsDBStatusBuilder().
		DbPath("/tmp/test-ExampleNutsDBStatus_newRecord").
		Build()
	if err != nil {
		panic(err)
	}

	// The record
	r := &EventRecord{
		Msg: "the msg",
		Tag: "the tag",
	}

	err = status.New(r)
	fmt.Println("Error id is missing:", err)

	r.AsyncIDs = []string{"id-1"}
	err = status.New(r)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Default timestamp implies event is evicted: %+v\n", status.Stats())

	r.Timestamp = time.Now()
	err = status.New(r)
	if err != nil {
		panic(err)
	}
	allIDs, err := status.AllIDs()
	if err != nil {
		panic(err)
	}
	fmt.Printf("IDs after event added: %v\n", allIDs)
	fmt.Printf("Stats: %+v\n", status.Stats())

	err = status.New(r)
	fmt.Println("Error ID exists:", err)

	r.AsyncIDs = []string{"id-2", "id-3"}
	err = status.New(r)
	fmt.Println("Error more than one ID:", err)

	err = status.Close()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Stats after close: %+v\n", status.Stats())

	// Cleant tmp path
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_newRecord")

	// Output:
	// Error id is missing: AsyncIDs must be formed by one and only one value
	// Default timestamp implies event is evicted: {BufferCount:0 Updated:0 Finished:0 Dropped:0 Evicted:1 DbIdxSize:0 DbMaxFileID:0 DbDataEntries:-1}
	// IDs after event added: [id-1]
	// Stats: {BufferCount:1 Updated:0 Finished:0 Dropped:0 Evicted:1 DbIdxSize:1 DbMaxFileID:0 DbDataEntries:-1}
	// Error ID exists: While update nutsdb: Record with id-1 id is present in status db
	// Error more than one ID: AsyncIDs must be formed by one and only one value
	// Stats after close: {BufferCount:0 Updated:0 Finished:0 Dropped:0 Evicted:0 DbIdxSize:0 DbMaxFileID:0 DbDataEntries:0}

}

func ExampleNutsDBStatus_getRecord() {

	// Ensure tmp path is clean
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_getRecord")

	status, err := NewNutsDBStatusBuilder().
		DbPath("/tmp/test-ExampleNutsDBStatus_getRecord").
		EventsTTLSeconds(1).
		Build()
	if err != nil {
		panic(err)
	}

	// The record
	r := &EventRecord{
		AsyncIDs:  []string{"id-1"},
		Timestamp: time.Now(),
		Msg:       "the msg",
		Tag:       "the tag",
	}

	err = status.New(r)
	if err != nil {
		panic(err)
	}
	allIDs, err := status.AllIDs()
	if err != nil {
		panic(err)
	}
	fmt.Printf("All IDs: %v\n", allIDs)

	theRecord, order, err := status.Get("id-1")
	fmt.Printf(
		"Order: %d, Error: %v, Record {AsyncIDs: %v, Tag: %s, Msg: %s}\n",
		order,
		err,
		theRecord.AsyncIDs,
		theRecord.Tag,
		theRecord.Msg)

	_, order, err = status.Get("id-2")
	fmt.Printf("ID does not exists, order: %d, err: %v\n", order, err)

	fmt.Printf("Stats: %+v\n", status.Stats())

	// Wait for expiration event
	time.Sleep(time.Millisecond * 1010)

	_, order, err = status.Get("id-1")
	fmt.Printf("Event expired: %d, err: %v\n", order, err)

	fmt.Printf("Stats: %+v\n", status.Stats())

	err = status.Close()
	if err != nil {
		panic(err)
	}

	// Cleant tmp path
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_getRecord")

	// Output:
	// All IDs: [id-1]
	// Order: 0, Error: <nil>, Record {AsyncIDs: [id-1], Tag: the tag, Msg: the msg}
	// ID does not exists, order: -1, err: EventRecord not found in index
	// Stats: {BufferCount:1 Updated:0 Finished:0 Dropped:0 Evicted:0 DbIdxSize:1 DbMaxFileID:0 DbDataEntries:-1}
	// Event expired: -1, err: EventRecord evicted
	// Stats: {BufferCount:0 Updated:0 Finished:0 Dropped:0 Evicted:1 DbIdxSize:0 DbMaxFileID:0 DbDataEntries:-1}
}

func ExampleNutsDBStatus_updateRecord() {

	// Ensure tmp path is clean
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_updateRecord")

	status, err := NewNutsDBStatusBuilder().
		DbPath("/tmp/test-ExampleNutsDBStatus_updateRecord").
		EventsTTLSeconds(1).
		Build()
	if err != nil {
		panic(err)
	}

	// The record
	r := &EventRecord{
		AsyncIDs:  []string{"id-1"},
		Timestamp: time.Now(),
		Msg:       "the msg",
		Tag:       "the tag",
	}

	err = status.New(r)
	if err != nil {
		panic(err)
	}

	err = status.Update("id-99", "id-100")
	fmt.Printf("Error update not existint record: %v\n", err)

	err = status.Update("id-1", "id-2")
	if err != nil {
		panic(err)
	}

	ids, err := status.AllIDs()
	if err != nil {
		panic(err)
	}
	fmt.Println("1 update: All ids", ids)

	er, order, err := status.Get("id-2")
	if err != nil {
		panic(err)
	}
	fmt.Printf("1 update: event record.AsyncIDs: er.AsyncIDs: %v, order: %d\n", er.AsyncIDs, order)

	err = status.Update("id-2", "id-3")
	if err != nil {
		panic(err)
	}

	ids, err = status.AllIDs()
	if err != nil {
		panic(err)
	}
	fmt.Println("2 update: All ids", ids)

	er, order, err = status.Get("id-3")
	if err != nil {
		panic(err)
	}
	fmt.Printf("2 update: event record.AsyncIDs: er.AsyncIDs: %v, order: %d\n", er.AsyncIDs, order)

	fmt.Printf("Stats: %+v\n", status.Stats())

	// Wait for expiration event
	time.Sleep(time.Millisecond * 1010)

	err = status.Update("id-3", "id-4")
	fmt.Println("Error when update after event expiration", err)

	fmt.Printf("Stats: %+v\n", status.Stats())

	err = status.Close()
	if err != nil {
		panic(err)
	}

	// Cleant tmp path
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_updateRecord")

	// Output:
	// Error update not existint record: EventRecord not found in index
	// 1 update: All ids [id-2]
	// 1 update: event record.AsyncIDs: er.AsyncIDs: [id-1 id-2], order: 0
	// 2 update: All ids [id-3]
	// 2 update: event record.AsyncIDs: er.AsyncIDs: [id-1 id-2 id-3], order: 0
	// Stats: {BufferCount:1 Updated:0 Finished:0 Dropped:0 Evicted:0 DbIdxSize:1 DbMaxFileID:0 DbDataEntries:-1}
	// Error when update after event expiration EventRecord evicted
	// Stats: {BufferCount:0 Updated:0 Finished:0 Dropped:0 Evicted:1 DbIdxSize:0 DbMaxFileID:0 DbDataEntries:-1}
}

func ExampleNutsDBStatus_findAll() {

	// Ensure tmp path is clean
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_findAll")

	status, err := NewNutsDBStatusBuilder().
		DbPath("/tmp/test-ExampleNutsDBStatus_findAll").
		EventsTTLSeconds(1).
		Build()
	if err != nil {
		panic(err)
	}

	// The record
	r := &EventRecord{
		Timestamp: time.Now(),
		Msg:       "the msg",
		Tag:       "the tag",
	}

	for i := 1; i <= 9; i++ {
		r.AsyncIDs = []string{fmt.Sprintf("id-%d", i)}
		err = status.New(r)
		if err != nil {
			panic(err)
		}
	}

	ids, err := status.AllIDs()
	if err != nil {
		panic(err)
	}
	fmt.Println("All ids", ids)

	ers, err := status.FindAll()
	if err != nil {
		panic(err)
	}
	for i, v := range ers {
		fmt.Printf("i: %d v.AssyncIds: %v\n", i, v.AsyncIDs)
	}

	// Wait for expiration event
	time.Sleep(time.Millisecond * 1010)
	ers, err = status.FindAll()
	fmt.Printf("All expired ers %+v, error: %v\n", ers, err)

	ids, err = status.AllIDs()
	if err != nil {
		panic(err)
	}
	fmt.Println("All ids after records expired", ids)

	fmt.Printf("Stats: %+v\n", status.Stats())

	err = status.Close()
	if err != nil {
		panic(err)
	}

	// Cleant tmp path
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_findAll")

	// Output:
	// All ids [id-1 id-2 id-3 id-4 id-5 id-6 id-7 id-8 id-9]
	// i: 0 v.AssyncIds: [id-1]
	// i: 1 v.AssyncIds: [id-2]
	// i: 2 v.AssyncIds: [id-3]
	// i: 3 v.AssyncIds: [id-4]
	// i: 4 v.AssyncIds: [id-5]
	// i: 5 v.AssyncIds: [id-6]
	// i: 6 v.AssyncIds: [id-7]
	// i: 7 v.AssyncIds: [id-8]
	// i: 8 v.AssyncIds: [id-9]
	// All expired ers [], error: <nil>
	// All ids after records expired []
	// Stats: {BufferCount:0 Updated:0 Finished:0 Dropped:0 Evicted:9 DbIdxSize:0 DbMaxFileID:0 DbDataEntries:-1}
}

func ExampleNutsDBStatus_finishRecord() {

	// Ensure tmp path is clean
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_finishRecord")

	status, err := NewNutsDBStatusBuilder().
		DbPath("/tmp/test-ExampleNutsDBStatus_finishRecord").
		Build()
	if err != nil {
		panic(err)
	}

	// The record
	r := &EventRecord{
		AsyncIDs:  []string{"id-1"},
		Timestamp: time.Now(),
		Msg:       "the msg",
		Tag:       "the tag",
	}

	// Add the record
	err = status.New(r)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Stats: %+v\n", status.Stats())

	// Finish the record
	err = status.FinishRecord("id-1")
	if err != nil {
		panic(err)
	}

	fmt.Printf("Stats after record finished: %+v\n", status.Stats())

	err = status.FinishRecord("id-1")
	fmt.Println("Error while finish record previously finished", err)

	ids, err := status.AllIDs()
	if err != nil {
		panic(err)
	}
	fmt.Println("All IDs", ids)

	// Cleant tmp path
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_finishRecord")

	// Output:
	// Stats: {BufferCount:1 Updated:0 Finished:0 Dropped:0 Evicted:0 DbIdxSize:1 DbMaxFileID:0 DbDataEntries:-1}
	// Stats after record finished: {BufferCount:0 Updated:0 Finished:1 Dropped:0 Evicted:0 DbIdxSize:0 DbMaxFileID:0 DbDataEntries:-1}
	// Error while finish record previously finished EventRecord not found in index
	// All IDs []
}

func ExampleNutsDBStatus_newRecord_bufferOverflow() {

	// Ensure tmp path is clean
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_newRecord_bufferOverflow")

	status, err := NewNutsDBStatusBuilder().
		BufferSize(1). // Force buffer size to 1
		DbPath("/tmp/test-ExampleNutsDBStatus_newRecord_bufferOverflow").
		Build()
	if err != nil {
		panic(err)
	}

	// The record template
	r := &EventRecord{

		Msg: "the msg",
		Tag: "the tag",
	}

	// Add record 1
	r.Timestamp = time.Now()
	r.AsyncIDs = []string{"id-1"}
	err = status.New(r)
	if err != nil {
		panic(err)
	}

	// Wait some millisencods to ensure timestamp of event 2 is significativally greater than on event 1
	time.Sleep(time.Millisecond * 50)

	// Add record 2
	r.Timestamp = time.Now()
	r.AsyncIDs = []string{"id-2"}
	err = status.New(r)
	if err != nil {
		panic(err)
	}

	// Print ids only event with id equal to id-2 must be present
	allIDs, err := status.AllIDs()
	if err != nil {
		panic(err)
	}
	fmt.Printf("IDs: %v\n", allIDs)
	fmt.Printf("Stats: %+v\n", status.Stats())

	status.Close()

	// Cleant tmp path
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_newRecord_bufferOverflow")

	// Output:
	// IDs: [id-2]
	// Stats: {BufferCount:1 Updated:0 Finished:0 Dropped:1 Evicted:0 DbIdxSize:1 DbMaxFileID:0 DbDataEntries:-1}

}

func ExampleNutsDBStatus_houseKeeping() {
	// Ensure tmp path is clean
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_houseKeeping")

	status, err := NewNutsDBStatusBuilder().
		DbSegmentSize(10240).    // To ensure several files will be created
		FilesToConsolidateDb(2). // To ensure that HouseKeeping call with generate files
		DbPath("/tmp/test-ExampleNutsDBStatus_houseKeeping").
		Build()
	if err != nil {
		panic(err)
	}

	// We create and finish 1000 event records of 64 bytes size this will ensure files are recreated
	// The record
	r := &EventRecord{
		AsyncIDs: []string{"id-1"},
		Msg:      strings.Repeat("X", 64),
		Tag:      "test.keep.free",
	}
	for i := 0; i < 1000; i++ {
		r.Timestamp = time.Now()
		// Add the record
		err = status.New(r)
		if err != nil {
			panic(err)
		}

		err = status.FinishRecord("id-1")
	}

	fmt.Println("BEFORE HouseKeeping")
	fmt.Printf(
		"Number of files: %d\n",
		NumberOfFiles("/tmp/test-ExampleNutsDBStatus_houseKeeping"))
	fmt.Printf("Stats: %+v\n", status.Stats())

	err = status.HouseKeeping()
	if err != nil {
		panic(err)
	}

	fmt.Println("AFTER HouseKeeping")
	fmt.Printf(
		"Number of files: %d\n",
		NumberOfFiles("/tmp/test-ExampleNutsDBStatus_houseKeeping"))
	fmt.Printf("Stats: %+v\n", status.Stats())

	status.Close()

	// Cleant tmp path
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_houseKeeping")

	// Output:
	// BEFORE HouseKeeping
	// Number of files: 55
	// Stats: {BufferCount:0 Updated:0 Finished:1000 Dropped:0 Evicted:0 DbIdxSize:0 DbMaxFileID:54 DbDataEntries:-1}
	// AFTER HouseKeeping
	// Number of files: 1
	// Stats: {BufferCount:0 Updated:0 Finished:1000 Dropped:0 Evicted:0 DbIdxSize:0 DbMaxFileID:55 DbDataEntries:-1}
}

func ExampleNutsDBStatus_rebuiltCorruptIdx() {
	// Ensure tmp path is clean
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_rebuiltCorruptIdx")

	status, err := NewNutsDBStatusBuilder().
		DbPath("/tmp/test-ExampleNutsDBStatus_rebuiltCorruptIdx").
		Build()
	if err != nil {
		panic(err)
	}

	// Create some records
	// The record template
	r := &EventRecord{
		Msg: strings.Repeat("X", 64),
		Tag: "test.keep.free",
	}
	for i := 0; i < 10; i++ {
		r.AsyncIDs = []string{
			fmt.Sprintf("id-%d", i),
		}
		r.Timestamp = time.Now()
		// Add the record
		err = status.New(r)
		if err != nil {
			panic(err)
		}
	}

	// Close status
	err = status.Close()
	if err != nil {
		panic(err)
	}

	// Manually corrupt the index
	opts := nutsdb.DefaultOptions
	opts.Dir = "/tmp/test-ExampleNutsDBStatus_rebuiltCorruptIdx"
	db, err := nutsdb.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Update(func(tx *nutsdb.Tx) error {
		return tx.Delete("idx", []byte("idx")) // See idxBucket constant and idxKey var with updated values
	})
	if err != nil {
		panic(err)
	}
	db.Close()

	// Reopen status and
	status, err = NewNutsDBStatusBuilder().
		DbPath("/tmp/test-ExampleNutsDBStatus_rebuiltCorruptIdx").
		Build()
	if err != nil {
		panic(err)
	}

	// Stats should display righ index value
	fmt.Printf("Stats: %+v\n", status.Stats())
	status.Close()

	// Cleant tmp path
	os.RemoveAll("/tmp/test-ExampleNutsDBStatus_rebuiltCorruptIdx")

	// Output:
	// Stats: {BufferCount:10 Updated:0 Finished:0 Dropped:0 Evicted:0 DbIdxSize:10 DbMaxFileID:0 DbDataEntries:-1}
}
