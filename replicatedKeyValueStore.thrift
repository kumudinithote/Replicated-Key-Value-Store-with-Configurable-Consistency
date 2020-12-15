typedef string Value
typedef i32 Key

exception SystemException {
  1: optional string message
}

struct Request {
	1: optional string timestamp;
	2: required string level;
	3: required bool isCoordinator;
}

struct ReplicaNode{
	1: string id;
	2: string ip;
	3: i32 port;
	4: i32 minKey;
	5: i32 maxKey;
}

service ReplicatedKeyValueStore {
  bool put(1: Key key,2: Value value, 3: Request request, 4: ReplicaNode replicaNode)
    throws (1: SystemException systemException),
  
  Value get(1: Key key, 2: Request request, 3: ReplicaNode replicaNode)
    throws (1: SystemException systemException),


}
