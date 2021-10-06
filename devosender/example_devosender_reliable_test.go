package devosender

import (
	"fmt"
	"os"
	"time"
)

func ExampleReliableClientBuilder_initErrors() {
	rc, err := NewReliableClientBuilder().Build()
	fmt.Println("1 error", err)
	fmt.Println("1 rc", rc)

	// Ensure path is clean
	os.RemoveAll("/tmp/test")
	rc, err = NewReliableClientBuilder().DbPath("/tmp/test").Build()
	fmt.Println("2 error", err)
	fmt.Println("2 rc", rc)

	// Output:
	// 1 error Empty path where persist status
	// 1 rc <nil>
	// 2 error Undefined inner client builder
	// 2 rc <nil>
}

func ExampleReliableClient_withoutConnection() {
	// Ensure path is clean
	os.RemoveAll("/tmp/test")

	rc, err := NewReliableClientBuilder().
		DbPath("/tmp/test").
		ClientBuilder(NewClientBuilder()).
		Build()

	fmt.Println("error", err)
	fmt.Println("rc.Client", rc.Client)
	fmt.Println("rc.IsStandBy", rc.IsStandBy())

	rc.SendWTagAsync("tag", "async msg")
	fmt.Printf("rc.Stats %+v\n", rc.Stats())

	err = rc.Flush()
	fmt.Printf("error flush: %+v\n", err)
	fmt.Printf("rc.Stats after flush %+v\n", rc.Stats())

	err = rc.Close()
	fmt.Printf("error close: %+v\n", err)
	fmt.Printf("rc.Stats after close %+v\n", rc.Stats())

	// Output:
	// error <nil>
	// rc.Client <nil>
	// rc.IsStandBy false
	// rc.Stats {Count:1 Updated:0 Finished:0 Dropped:0 Evicted:0}
	// error flush: <nil>
	// rc.Stats after flush {Count:1 Updated:0 Finished:0 Dropped:0 Evicted:0}
	// error close: <nil>
	// rc.Stats after close {Count:0 Updated:0 Finished:0 Dropped:0 Evicted:0}

}

func ExampleReliableClient() {
	// Ensure path is clean
	os.RemoveAll("/tmp/test")

	rc, err := NewReliableClientBuilder().
		DbPath("/tmp/test").
		ClientBuilder(
			NewClientBuilder().
				EntryPoint("udp://example.com:80"),
		).Build()

	fmt.Println("error", err)
	fmt.Println("rc.GetEntryPoint", rc.GetEntryPoint())
	fmt.Println("rc.IsStandBy", rc.IsStandBy())

	rc.SendWTagAsync("tag", fmt.Sprintf("async msg at %s", time.Now()))
	fmt.Printf("rc.Stats %+v\n", rc.Stats())

	err = rc.Flush()
	fmt.Printf("error flush: %+v\n", err)
	fmt.Printf("rc.Stats after flush %+v\n", rc.Stats())

	err = rc.Close()
	fmt.Printf("error close: %+v\n", err)

	// Output:
	// error <nil>
	// rc.GetEntryPoint udp://example.com:80
	// rc.IsStandBy false
	// rc.Stats {Count:1 Updated:0 Finished:0 Dropped:0 Evicted:0}
	// error flush: <nil>
	// rc.Stats after flush {Count:0 Updated:0 Finished:1 Dropped:0 Evicted:0}
	// error close: <nil>
}

func ExampleReliableClient_standbyAndWakeUp() {
	// Ensure path is clean
	os.RemoveAll("/tmp/test")

	rc, err := NewReliableClientBuilder().
		DbPath("/tmp/test").
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://example.com:80"),
		).
		RetryDaemonWaitBtwChecks(time.Millisecond * 100).
		RetryDaemonInitDelay(time.Millisecond * 50).
		Build()
	if err != nil {
		panic(err)
	}

	// Pass to inactive
	err = rc.StandBy()
	fmt.Println("StandBy error", err)

	// Send data on stand by mode
	rc.SendWTagAsync("tag", fmt.Sprintf("async msg at %s", time.Now()))
	fmt.Printf("rc.Stats %+v\n", rc.Stats())

	rc.Flush()
	fmt.Printf("rc.Stats after flush %+v\n", rc.Stats())

	err = rc.WakeUp()
	fmt.Println("WakeUp error", err)
	// Waiting for ensure damon is retrying
	time.Sleep(time.Millisecond * 300)
	fmt.Printf("rc.Stats after Wakeup and wait %+v\n", rc.Stats())

	rc.Close()
	fmt.Printf("rc.Stats after closed %+v\n", rc.Stats())

	// Output:
	// StandBy error <nil>
	// rc.Stats {Count:1 Updated:0 Finished:0 Dropped:0 Evicted:0}
	// rc.Stats after flush {Count:1 Updated:0 Finished:0 Dropped:0 Evicted:0}
	// WakeUp error <nil>
	// rc.Stats after Wakeup and wait {Count:0 Updated:1 Finished:1 Dropped:0 Evicted:0}
	// rc.Stats after closed {Count:0 Updated:0 Finished:0 Dropped:0 Evicted:0}
}

func ExampleReliableClient_evicted() {
	// Ensure path is clean
	os.RemoveAll("/tmp/test")

	rc, err := NewReliableClientBuilder().
		DbPath("/tmp/test").
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://example.com:80"),
		).
		RetryDaemonWaitBtwChecks(time.Millisecond * 100).
		EventTimeToLiveInSeconds(1). // Event expiration  to 1 second
		Build()
	if err != nil {
		panic(err)
	}

	// Pass to inactive
	err = rc.StandBy()
	fmt.Println("StandBy error", err)

	// Send data on stand by mode
	rc.SendWTagAsync("tag", fmt.Sprintf("async msg at %s", time.Now()))
	fmt.Printf("rc.Stats %+v\n", rc.Stats())

	// Waiting for ensure event was expired
	time.Sleep(time.Millisecond * 1100)

	err = rc.WakeUp()
	fmt.Println("WakeUp error", err)
	fmt.Printf("rc.Stats after Wakeup and wait %+v\n", rc.Stats())

	err = rc.Flush()
	fmt.Println("Flush error", err)
	fmt.Printf("rc.Stats after Flush and wait %+v\n", rc.Stats())

	rc.Close()
	fmt.Printf("rc.Stats after closed %+v\n", rc.Stats())

	// Output:
	// StandBy error <nil>
	// rc.Stats {Count:1 Updated:0 Finished:0 Dropped:0 Evicted:0}
	// WakeUp error <nil>
	// rc.Stats after Wakeup and wait {Count:1 Updated:0 Finished:0 Dropped:0 Evicted:0}
	// Flush error <nil>
	// rc.Stats after Flush and wait {Count:0 Updated:0 Finished:1 Dropped:0 Evicted:1}
	// rc.Stats after closed {Count:0 Updated:0 Finished:0 Dropped:0 Evicted:0}
}

func ExampleReliableClient_dropped() {
	// Ensure path is clean
	os.RemoveAll("/tmp/test")

	rc, err := NewReliableClientBuilder().
		DbPath("/tmp/test").
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://example.com:80"),
		).
		BufferEventsSize(2).
		Build()
	if err != nil {
		panic(err)
	}

	// Pass to inactive
	err = rc.StandBy()
	fmt.Println("StandBy error", err)

	// Send data on stand by mode
	rc.SendWTagAsync("tag", fmt.Sprintf("async msg 1 at %s", time.Now()))
	rc.SendWTagAsync("tag", fmt.Sprintf("async msg 2 at %s", time.Now()))
	rc.SendWTagAsync("tag", fmt.Sprintf("async msg 3 at %s", time.Now()))
	rc.SendWTagAsync("tag", fmt.Sprintf("async msg 4 at %s", time.Now()))
	rc.SendWTagAsync("tag", fmt.Sprintf("async msg 5 at %s", time.Now()))
	fmt.Printf("rc.Stats %+v\n", rc.Stats())

	err = rc.WakeUp()
	fmt.Println("WakeUp error", err)
	fmt.Printf("rc.Stats after Wakeup and wait %+v\n", rc.Stats())

	// There should be Count: 2 after flush, because we do not have enough time
	// to daemon to check that records were propertly resend
	err = rc.Flush()
	fmt.Println("Flush error", err)
	fmt.Printf("rc.Stats after Flush and wait %+v\n", rc.Stats())

	rc.Close()
	fmt.Printf("rc.Stats after closed %+v\n", rc.Stats())

	// Output:
	// StandBy error <nil>
	// rc.Stats {Count:2 Updated:0 Finished:3 Dropped:3 Evicted:0}
	// WakeUp error <nil>
	// rc.Stats after Wakeup and wait {Count:2 Updated:0 Finished:3 Dropped:3 Evicted:0}
	// Flush error <nil>
	// rc.Stats after Flush and wait {Count:2 Updated:2 Finished:3 Dropped:3 Evicted:0}
	// rc.Stats after closed {Count:0 Updated:0 Finished:0 Dropped:0 Evicted:0}
}

func ExampleReliableClient_nilInnerClient() {
	// Ensure path is clean
	os.RemoveAll("/tmp/test")

	rc, err := NewReliableClientBuilder().
		DbPath("/tmp/test").
		ClientBuilder(
			NewClientBuilder(),
		).
		Build()

	// Call all inherit funcs
	err = rc.AddReplaceSequences("old", "new")
	fmt.Println("AddReplaceSequences:", err)
	rc.SetSyslogHostName("hostname")
	fmt.Println("Nothing to do when call SetSyslogHostName")
	err = rc.SetDefaultTag("old")
	fmt.Println("SetDefaultTag:", err)
	err = rc.Send("msg")
	fmt.Println("Send:", err)
	err = rc.SendWTag("tag", "msg")
	fmt.Println("SendWTag:", err)
	s := rc.SendAsync("msg")
	fmt.Printf("SendAsync returns not empty id: '%v'\n", s != "")
	s = rc.SendWTagAsync("tag", "msg")
	fmt.Printf("SendWTagAsync returns not empty id: '%v'\n", s != "")
	s = rc.SendWTagAndCompressorAsync("tag", "msg", nil)
	fmt.Printf("SendWTagAndCompressorAsync returns not empty id: '%v'\n", s != "")
	err = rc.WaitForPendingAsyncMessages()
	fmt.Println("WaitForPendingAsyncMessages:", err)
	err = rc.WaitForPendingAsyncMsgsOrTimeout(0)
	fmt.Println("WaitForPendingAsyncMsgsOrTimeout:", err)
	m := rc.AsyncErrors()
	fmt.Println("AsyncErrors:", m)
	i := rc.AsyncErrorsNumber()
	fmt.Printf("AsyncErrorsNumber: '%d'\n", i)
	s = rc.GetEntryPoint()
	fmt.Printf("GetEntryPoint returns: '%s'\n", s)
	ss := rc.AsyncIds()
	fmt.Printf("AsyncIds returns nil: '%v'\n", ss == nil)
	b := rc.IsAsyncActive("")
	fmt.Printf("IsAsyncActive returns: %v\n", b)
	i = rc.AsyncsNumber()
	fmt.Printf("AsyncsNumber returns: '%d'\n", i)
	t := rc.LastSendCallTimestamp()
	fmt.Printf("LastSendCallTimestamp returns: '%v'\n", t)
	i, err = rc.Write([]byte{})
	fmt.Println("Write i:", i, "err:", err)

	err = rc.Close()
	fmt.Println("Close:", err)
	fmt.Printf("rc.Stats after closed %+v\n", rc.Stats())

	// Output:
	// AddReplaceSequences: Receiver func call with nil pointer
	// Nothing to do when call SetSyslogHostName
	// SetDefaultTag: Receiver func call with nil pointer
	// Send: Receiver func call with nil pointer
	// SendWTag: Receiver func call with nil pointer
	// SendAsync returns not empty id: 'true'
	// SendWTagAsync returns not empty id: 'true'
	// SendWTagAndCompressorAsync returns not empty id: 'true'
	// WaitForPendingAsyncMessages: Receiver func call with nil pointer
	// WaitForPendingAsyncMsgsOrTimeout: Receiver func call with nil pointer
	// AsyncErrors: map[:Receiver func call with nil pointer]
	// AsyncErrorsNumber: '0'
	// GetEntryPoint returns: ''
	// AsyncIds returns nil: 'true'
	// IsAsyncActive returns: false
	// AsyncsNumber returns: '0'
	// LastSendCallTimestamp returns: '0001-01-01 00:00:00 +0000 UTC'
	// Write i: 0 err: Receiver func call with nil pointer
	// Close: <nil>
	// rc.Stats after closed {Count:0 Updated:0 Finished:0 Dropped:0 Evicted:0}
}