# devo-go
Golang module to work with Devo (https://www.devo.com/)

# Packages in module

## devoquery

Package with tools and helper functions to run LinQ queries using Devo API V2. See [Build a query using LINQ](https://docs.devo.com/confluence/ndt/searching-data/building-a-query/build-a-query-using-linq)

See next code examples:
* [querytoken.go](./examples/querytoken.go) Query to Devo authenticated with token

## devosender

Package with tools to send data to Devo to central relay or through relay-in-house.

See next code examples:
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
