# pubsure-zk

This library is an implementation of both the `DirectoryReader` and `DirectoryWriter` pubsure protocols, using Zookeeper as its backend. The source URIs are stored as ephemeral nodes. It has automatic reconnection logic build in, reinserting the ephemeral nodes when the Zookeeper cluster had expired the client.

## Integration

Add `[pubsure/pubsure-zk "0.1.0-SNAPSHOT"]` to your dependencies in Leiningen.

## Starting and stopping

1. Call `(def zkdir (pubsure-zk.directory/start-directory "<host1>:<port1>,..."))`. The return value is used for stopping the directory. One can supply some options to this function:
   * `:timeout-msec` - The number of milliseconds until the internal Zookeeper client is expired and the registered URIs through this `DirectoryWriter` are removed automatically. Default is 10000 (10 seconds).
   * `:zk-root` - The root path of where to store the topics and the source URIs. Default is `/pubsure`.
   * `:subscribe-buffer` - The size of the sliding core.async buffer used in the `DirectoryReader/watch-sources` function, in case no core.async channel is supplied. Default is 100.
2. Call `(pubsure-zk.directory/stop-directory zkdir)` to stop this directory service reader/writer instance. 

## Usage

Now you can use the `DirectoryReader` and `DirectoryWriter` protocol functions on `zkdir`. For more information about this API, see the [pubsure-core](#) project's README. One can also use the [pubsure-ws](#) project for example, to expose the `zkdir` instance via the [WAMP](#) specification over a Websocket.


## License

Copyright © 2014

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
