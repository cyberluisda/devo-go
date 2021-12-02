# devo-go
Golang module to work with Devo (https://www.devo.com/)

# Packages in module

## devoquery

Package with tools and helper functions to run LinQ queries using Devo API V2. See [Build a query using LINQ](https://docs.devo.com/confluence/ndt/searching-data/building-a-query/build-a-query-using-linq)

See next code examples:
* [querytoken.go](./examples/querytoken.go) Query to Devo authenticated with token

## devosender

Package with tools to send data to Devo to central relay or through relay-in-house.

On one hand there are two main interfaces to get an abstraction of sender events:
* `DevoSender`: This interface defines the basic element to send events to Devo or relay in-house.
* `SwitchDevoSender`: This interface defines and element that send events to Devo but can be paused (cancel and close connection to relay in-house) with `StandBy` function and reconnect again with `WakeUp` function. Events that are send using `SendXXXAxync` functions while sender is in _stand by mode_ must be conserved by each implementation in order to be sent when sender leave this _stand by mode_. If the user call no `Async` functions to send events during _stand by mode_ period, error will be returned.

On the other hand we have three main implementations or _Clients_ that are the engine that send data to Devo (or relay):
* `Client` type. It is in an implementation of `DevoSender` interface. Recommended way to instate it is using `ClientBuilder` that follows _Builder_ pattern. The recommended way to instantiate `ClientBuilder` is using `NewClientBuilder` factory function.

* `LazyClient` type. It is in an implementation of `SwitchDevoSender` interface. Recommended way to instate it is using `LazyClientBuilder` that follows _Builder_ pattern. The recommended way to instantiate `LazyClientBuilder` is using `NewLazyClientBuilder` factory function.

    Events send with `SendXXXAsync` functions during _stand by mode_ are saved in a **memory** buffer. These events will be send when `WakeUp` or `Close` functions are called.

* `ReliableClient` type. It is in an implementation of `SwitchDevoSender` interface. Recommended way to instate it is using `ReliableClientBuilder` that follows _Builder_ pattern. The recommended way to instantiate `ReliableClientBuilder` is using `NewReliableClientBuilder` factory function.

    This component save all events send with `SendXXXAsync` functions in a persistence layer on disk. Those events will be re-send when client is not in _stand by mode_ by an internal Daemon without any direct user action.

    The client support recover the status from disk if client is stopped (abruptly or not) and started again. Events that returned error will be tried to resend too. The max number of events to save on disk are defined by two parameters set with builder functions:

    - `EventTimeToLiveInSeconds` That defines the expiration time, in seconds, of the event (max time to live in the status)

    - `BufferEventsSize`. Defines the max number of the events to get in the status at same time.

See go doc for more info.

This projecet include a set of examples to guide as introduction to the main components:
* [senderclientclean.go](./examples/senderclientclean.go) Example to send data to Devo relay-in house
* [sendercentralrelay.go](./examples/sendercentralrelay.go) Example to send data to Devo central relay
* [senderasync.go](./examples/senderasync.go) Example to send data to Devo relay-in house in _asynchronous_ mode
* [senderfileasevent.go](./examples/senderfileasevent.go) Example to send content of a file as only one event to Devo relay-in house. Using `io.Copy` func
* [sendercentralrelaywithbuilder.go](./examples/sendercentralrelaywithbuilder.go) Example to send data to Devo central relay but instantiating client using `ClientBuilder`
* [sendercentralrelaytimeout.go](./examples/sendercentralrelaytimeout.go) Example to send data to Devo central relay that you can set TCP connection timeout based on `ClientBuilder` option
* [sendercentralrelaykeepalive.go](./examples/sendercentralrelaykeepalive.go) Example to send data to Devo central relay but setting TCP keep-alive time based on `ClientBuilder` option
* [sendercentralrelayrecreatecon.go](./examples/sendercentralrelayrecreatecon.go) Example to send data to Devo central relay but set to recreate connection if time from last event sent is greater than threshold. This value is set using `ClientBuilder` option.

See [./examples](./examples) for more example files.

## devologtable

Package to abstract a Key Value structure (log table) using Devo tables as a persistence.

See next code examples:
* [logtable.go](./examples/logtable.go) Example to add, delete, get and dump values using `logtable` abstraction
* [logtable_batch.go](./examples/logtable_batch.go) Example to add/set a batch of values in a `logtable`
* [logtable_prefixnames.go](./examples/logtable_prefixnames.go) Example to list and optionally delete a batch of values in a `logtable`

See [./examples](./examples) for more example files.

# Running examples

To run examples you need to get Golang installed and configured. See [Golang official site](https://golang.org/) for more info.

Then you can run examples executing next line at root project path:
```bash
go run examples/EXAMPLE_FILE.go
```

And follow simple help displayed in case of required parameters
