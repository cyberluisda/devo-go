# devo-go
Golang module to work with Devo (https://www.devo.com/)

# Packages in module

## devoquery

Package with tools and helper functions to run LinQ queries using Devo API V2. See [Build a query using LINQ](https://docs.devo.com/confluence/ndt/searching-data/building-a-query/build-a-query-using-linq)

See next code examples:
* [querytoken.go](./examples/querytoken.go) Query to Devo authenticated with token

## devosender

Package with tools to send data to Devo to central relay or through relay-in-house.