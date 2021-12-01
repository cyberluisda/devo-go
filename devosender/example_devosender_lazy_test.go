package devosender

import (
	"fmt"
	"time"
)

func ExampleLazyClientBuilder_initErrors() {
	lcb := &LazyClientBuilder{}
	lc, err := lcb.Build()
	fmt.Println("1: error", err, "- LazyClient", lc)

	lcb.ClientBuilder(NewClientBuilder())
	lc, err = lcb.Build()
	fmt.Println("2: error", err, "- LazyClient", lc)

	lcb.BufferSize(123)
	lc, err = lcb.Build()
	fmt.Println("3: error", err, "- LazyClient", lc)

	lcb.FlushTimeout(time.Second)
	lc, err = lcb.Build()
	fmt.Println("4: error", err, "- LazyClient", lc)

	// Output:
	// 1: error Undefined inner client builder - LazyClient <nil>
	// 2: error Buffer size less than 1 - LazyClient <nil>
	// 3: error Flush timeout empty or negative - LazyClient <nil>
	// 4: error Error while initialize client: Error when create new DevoSender (Clear): Entrypoint can not be empty - LazyClient <nil>

}

func ExampleLazyClientBuilder() {
	lcb := &LazyClientBuilder{}
	lc, err := lcb.
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")).
		BufferSize(256).
		FlushTimeout(time.Second).
		Build()
	lc, err = lcb.Build()

	// Only print first n characters to easy test with output
	lcStr := lc.String()[:120]
	fmt.Println("error", err, "- LazyClient", lcStr, "...")

	// Output:
	// error <nil> - LazyClient bufferSize: 256, standByMode: false, #eventsInBuffer: 0, flushTimeout: 1s, Client: {entryPoint: 'udp://localhost:13000', ...
}

func ExampleNewLazyClientBuilder() {

	lc, err := NewLazyClientBuilder().Build()
	fmt.Println("1: error", err, "- LazyClient", lc)

	lc, err = NewLazyClientBuilder().
		ClientBuilder(
			NewClientBuilder().EntryPoint("udp://localhost:13000")).
		Build()
	// Only print first n characters to easy test with output
	lcStr := lc.String()[:123]
	fmt.Println("2: error", err, "- LazyClient", lcStr, "...")

	// Output:
	// 1: error Undefined inner client builder - LazyClient <nil>
	// 2: error <nil> - LazyClient bufferSize: 256000, standByMode: false, #eventsInBuffer: 0, flushTimeout: 2s, Client: {entryPoint: 'udp://localhost:13000', ...
}
