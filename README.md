# Replicator

This is a sample data replicator application for Starcounter. It uses the `Starcounter.TransactionLog` assembly from the `pnext` version of Starcounter to scan the transaction log and replicate the transactions to and from other Starcounter databases running the Replicator application. The transport used is WebSockets.

The intent is for this source code to act as a "Hello world" sample for Starcounter developers who need to establish data streams between databases. The Replicator does not provide distributed transactions, distributed locking or consensus protocols.

## Requirements

All databases participating in replication require:
* A version of Starcounter that has the `Starcounter.TransactionLog` assembly.
* A unique Object ID range (can be set when creating the database using the `Advanced` settings).
* All except one require the Replicator URI for it's upstream (parent) replication database.
* You should run the same versions of all applications whose data are being replicated.

## Topology

This Replicator application assumes that all databases participating in replication can be organized into a tree structure, as each database is allowed one URI to designate it's "parent" database, but may have any number of "children". This parent-child relationship does not regulate data flow; it only specifies that the child database is responsible for maintaining the WebSocket connection to it's parent. Once connected, data flow is bidirectional.

The Replicator will prevent local feedback loops, meaning that when an incoming replicated transaction is applied to the database, the resulting local transaction will not be sent back to the sender (but it will be sent to other connected databases). The Replicator cannot prevent application-level feedback loops caused by commit hooks or similar mechanisms. It also cannot prevent loops if the topology itself contains loops (if any node has itself as a parent somewhere in the chain).

## Filtering

The `TransactionLog` API will provide all non-system table transactions from a given log position. This includes transactions which may no longer be possible to perform. For instance, if the database class (table) or property (column) no longer exists. Also, it is usually desirable to filter out data which should not be distributed. This may be for a variety of reasons including legal, security, financial or simply to save bandwidth.

The Replicator has a filtering in place, which is an opt-in mechanism using Starcounter handlers. For example, to allow updates for the table "MyCompany.MyApplication" to be sent out, you add a handler like one of these:

> `Handle.GET("/Replicator/out/MyCompany.MyApplication/{?}", (string destinationGuid) => { return 200; });`
> `Handle.POST("/Replicator/out/MyCompany.MyApplication/update/{?}", (string destinationGuid) => { return 200; });`

The GET handler would be for the simple use-case of allowing or denying all changes to a given table using only the destination database GUID, and is fairly cheap to call. The POST handler can handle more complex scenarios, and receives a serialized `Starcounter.TransactionLog.update_record_entry` in the body.

The handler must status code `200 OK` to allow sending it, with an optional new `update_record_entry` in the body to send that instead, or `201 No Content` or higher to prevent it from being sent at all. The Replicator will also cache calls that return a `404 Not Found` to improve performance.

To send all transactions for all tables you would use the handler
> `Handle.GET("/Replicator/out/{?}/{?}", (string tableName, string destinationGuid) => { return 200; });`

## Use as backup solution

You can use the Replicator to maintain a real-time copy of a database by simply allowing all changes to be propagated to the copy and not doing any local changes on the copy.

## Footgun capabilities

Replicating data across a distributed system is an excellent [footgun](http://www.urbandictionary.com/define.php?term=footgun). It's easy to end up in a situation where the global system state is inconsistent. How to organize your data flows to prevent this is outside the scope of this README, but some general advice may be in order to help prevent inconsistencies:
* Allow only one database to make changes to a certain table (or column) and all others only read it.
* Do not delete or rename replicated database classes, as they will still be referenced in old transactions.
* Do not remove replicated database class properties, for the same reason.
* Don't start replication until all applications whose data being replicated are fully loaded on the codehost.
