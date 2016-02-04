# LogStreamer

This is a sample log streamer application for Starcounter. It uses the `Starcounter.TransactionLog` assembly from the `pnext` version of Starcounter to scan the transaction log and stream the transactions using WebSockets to and from other Starcounter databases running the LogStreamer application.

The intent is for this source code to act as a "Hello world" sample for Starcounter developers who need to establish data streams between databases. The LogStreamer does not provide distributed transactions, distributed locking or consensus protocols.

## Requirements

All databases using log streaming require:
* A version of Starcounter that has the `Starcounter.TransactionLog` assembly.
* A unique Object ID range (can be set when creating the database using the `Advanced` settings).
* All except one require the LogStreamer URI for it's upstream (parent) database.
* You must run the same versions of all applications whose data is being streamed.

## Topology

The LogStreamer application assumes that all databases participating can be organized into a tree structure.Each database is allowed one URI to designate it's "parent" database, but may have any number of "children". This parent-child relationship does not regulate data flow; it only specifies that the child database is responsible for maintaining the WebSocket connection to it's parent. Once connected, data flow is bidirectional (but not nessecarily symmetric).

The LogStreamer will prevent local feedback loops, meaning that when a received transaction is applied to the database, the resulting local transaction will not be sent back to the sender (but it will be sent to other connected databases). The LogStreamer cannot prevent application-level feedback loops caused by commit hooks or similar mechanisms, though it will by default request that it's transactions are run with no commit hooks active. It also cannot prevent loops if the topology itself contains loops (if any node has itself as a parent somewhere in the chain).

## Selection

The LogStreamer needs to know which database classes are to be sent. While it is possible to run without a whitelist, sending everything is only useful to maintain a failover machine or a very simple application. For most use cases, you need to identify the set of database classes that should be sent and supply a list of them when instantiating the LogStreamer.

Constructing the whitelist is outside the scope of the LogStreamer. In fact, it's literally impossible for it to know what's safe or not to send. To give you an idea of where to start, if you know the top level namespaces that your set of applications are using, you can find their database classes using `SELECT FullClassName FROM Starcounter.Metadata.ClrClass WHERE FullClassName LIKE "MyApplication.%"`. And remember it's better to start sending too little than too much.

So why is it bad to send too much? Because if you send over something that shouldn't have been, it is probably going to be very difficult to analyze what it was and how to safely undo it. On the other hand, if you find out you need to send something more, adding it to the whitelist and restarting the LogStreamer should be enough.

## Filtering

The `TransactionLog` API will provide all non-system table transactions from a given log position. This includes transactions which may no longer be possible to perform. For instance, if the database class (table) or property (column) no longer exists. Also, it is usually desirable to filter out data which should not be distributed. This may be for a variety of reasons including legal, security, financial or simply to save bandwidth.

The LogStreamer will provide an `IOperationFilter` interface you can implement and provide an object to a LogStreamer instance which will then let that object to do filtering on outbound data.

## Use as backup solution

You can use the LogStreamer to maintain a real-time copy of a database by simply allowing all changes to be propagated to the copy and not doing any local changes on the copy.

## Footgun capabilities

Copying data across a distributed system is an excellent [footgun](http://www.urbandictionary.com/define.php?term=footgun). It's easy to end up in a situation where the global system state is inconsistent. How to organize your data flows to prevent this is outside the scope of this README, but some general advice may be in order to help prevent inconsistencies:
* Allow only one database to make changes to a certain table (or column) and all others only read it.
* Do not delete or rename database classes that have already been sent, as they will still be referenced in old transactions.
* Do not remove database class properties that have already been sent, for the same reason.
* Don't start streaming until all applications whose database classes are being streamed are fully loaded on the codehost.

