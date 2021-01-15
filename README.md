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
* [senderclean.go](./examples/senderclean.go) Example to send data to Devo relay-in house
* [sendercentralrelay.go](./examples/sendercentralrelay.go) Example to send data to Devo central relay
* [senderasync.go](./examples/senderasync.go) Example to send data to Devo relay-in house in _asynchronous_ mode
* [senderfileasevent.go](./examples/senderfileasevent.go) Example to send content of a file as only one event to Devo relay-in house. Using `io.Copy` func

## devologtable

Package to abstract a Key Value structure (log table) using Devo tables as a persistence.

See next code examples:
* [logtable.go](./examples/logtable.go) Example to add, delete, get and dump values using `logtable` abstraction

# Running examples

To run examples you need to get Golang installed and configured. See [Golang official site](https://golang.org/) for more info.

Then you can run examples executing next line at root project path:
```bash
go run examples/EXAMPLE_FILE.go
```

And follow simple help displayed in case of required parameters
