# Replicated-Key-Value-Store-with-Configurable-Consistency
Replicated Key-Value Store with Configurable Consistency similar to Cassandra

Programming is done in Java.

How to run the code:

1. git clone <git_path>
2. cd cs457-557-fall2020-pa3-kumudinithote/Replicated Key-Value Store/src/
3. Compile code : make
4. start server instances : ./server.sh <port #> <replicas.txt> (run this command for all the 4 server)
5. Start client : ./client.sh <replicas.txt>

The format of replicas.txt is as follows:
<replicaName> <ip#> <port#>
Ex: 1 192.168.0.6 9000

Keys are unsigned integers between 0 and 255. Keys are assigned to replica servers using a partitioner similar to the ByteOrderedPartitioner in Cassandra.
The replication factor is 3 – every key-value pair should be stored on three out of four replicas. 
Get and Put method with the consistency level one and quorum has been implemented.
Implemented Hinted handoff mechanisum. 
Implemented a client that issues a stream of get and put requests to the key-value store which acts as a console. The client selects one arbitrary replica server as the coordinator for all its requests.
