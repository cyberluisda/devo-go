# 0.1.2
**Release date**: 2021-12-03

**Features**
* [#25](https://github.com/cyberluisda/devo-go/pull/25):  `FlushTimeout` in `ReliableClientBuilder` and `LazyClientBuilder` applies only with values greater than `0`

# 0.1.1
**Release date**: 2021-12-03

**Features**
* [#24](https://github.com/cyberluisda/devo-go/pull/24):  `LastSendCallTimestamp` added to `SwitchDevoSender` interface

# 0.1.0
**Release date**: 2021-12-02

**Bugs**
* [#15](https://github.com/cyberluisda/devo-go/issues/15): Inventory of pending async send operations are not propertly filled in devosender

**Features**
* [#23](https://github.com/cyberluisda/devo-go/pull/22): Examples and README improvements
* [#22](https://github.com/cyberluisda/devo-go/pull/22): New Client implementation: `LazyClient` to send data to Devo in without forced to have a permanent connection to Devo / relay in house
* [#21](https://github.com/cyberluisda/devo-go/pull/21): New Client implementation: `ReliableClient` to send data to Devo in a more robust way
* [#20](https://github.com/cyberluisda/devo-go/pull/20): Now you can enable payload compression (gzip) when send data to Devo
* [#17](https://github.com/cyberluisda/devo-go/pull/17): `LastSendCallTimestamp` function implemented in `devosender` module
* [#14](https://github.com/cyberluisda/devo-go/pull/14): `WaitForPendingAsyncMsgsOrTimeout` function implemented in `devosender` module
* [#13](https://github.com/cyberluisda/devo-go/pull/13): Several improvements in `devosender` module
* [#12](https://github.com/cyberluisda/devo-go/pull/12): `DevoSender` exposes _Async IDs_
* [#11](https://github.com/cyberluisda/devo-go/pull/11): `DevoSender` now can force create new connection after certain time of inactivity
* [#10](https://github.com/cyberluisda/devo-go/pull/10):
  - Allow set `Timeout` and `KeepAlive` TCP parameters when open connection in `DevoSender`
  - `DevoSender` code restructured to use Builder when instantiate clients.
* [#9](https://github.com/cyberluisda/devo-go/pull/9):
  - New batch operations in `devologtable` module.
  - Initial version of unit tests for `devologtable` module
* [#8](https://github.com/cyberluisda/devo-go/pull/8): Client builder implemented in Devo sender
* [#7](https://github.com/cyberluisda/devo-go/pull/7): `devosender` now follows io.WriteCloser interface
* [#6](https://github.com/cyberluisda/devo-go/pull/6): Unit tests for `devosender` package
* [#5](https://github.com/cyberluisda/devo-go/pull/5): Initial implementation of `devologtable` package
* [#4](https://github.com/cyberluisda/devo-go/pull/4): Asynchronous methods implemented in `devosender`
* [#3](https://github.com/cyberluisda/devo-go/pull/3): Unit tests implementation for `devoquery` package
* [#2](https://github.com/cyberluisda/devo-go/pull/2): Initial implementation of `devosender` package
* [#1](https://github.com/cyberluisda/devo-go/pull/1): Initial version of `devoquery` package