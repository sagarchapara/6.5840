package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Pair struct {
	key   int64
	value string
}

type ClientCache struct {
	values map[int64]Pair
}

func (cache *ClientCache) Get(clientId int64, correlationId int64) (string, bool) {
	value, ok := cache.values[clientId]

	if !ok {
		return "", false
	}

	if value.key == correlationId {
		return value.value, true
	}

	//remove the entry
	delete(cache.values, clientId)

	//log.Println("Removed Cache size: ", len(cache.values))

	return "", false
}

func (cache *ClientCache) Put(clientId int64, correlationId int64, value string) {
	//Always make new map as there is only one value per client
	cache.values[clientId] = Pair{key: correlationId, value: value}

	//log.Println("Added Cache size: ", len(cache.values))
}

func (cache *ClientCache) Remove(clientId int64) {
	delete(cache.values, clientId)

	//log.Println("Removed Cache size: ", len(cache.values))
}

// type LRUCache struct {
// 	values     map[int64]string
// 	mostRecent []int64
// }

// func (cache *LRUCache) Get(key int64) (string, bool) {
// 	value, ok := cache.values[key]

// 	return value, ok
// }

// func (cache *LRUCache) Put(key int64, value string) {
// 	cache.values[key] = value

// 	//update most recent
// 	if len(cache.mostRecent) >= 100 {
// 		cache.mostRecent = cache.mostRecent[1:]
// 		cache.mostRecent = append(cache.mostRecent, key)
// 	}
// }

type KVServer struct {
	mu sync.Mutex

	kvstore map[string]string

	opcache ClientCache
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.opcache.Remove(args.ClientId)

	value, ok := kv.kvstore[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//check if the request is duplicate
	if val, ok := kv.opcache.Get(args.ClientId, args.CorrelationId); ok {
		reply.Value = val
		return
	}

	//get prev value of the key
	prevVal, ok := kv.kvstore[args.Key]

	kv.kvstore[args.Key] = args.Value

	//update the cache
	kv.opcache.Put(args.ClientId, args.CorrelationId, prevVal)

	if ok {
		reply.Value = prevVal
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//check if the request is duplicate
	if val, ok := kv.opcache.Get(args.ClientId, args.CorrelationId); ok {
		reply.Value = val
		return
	}

	//get prev value of the key
	prevVal, ok := kv.kvstore[args.Key]

	if ok {
		kv.kvstore[args.Key] = prevVal + args.Value
		reply.Value = prevVal
	} else {
		kv.kvstore[args.Key] = args.Value
		reply.Value = ""
	}

	//update the cache
	kv.opcache.Put(args.ClientId, args.CorrelationId, prevVal)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvstore = make(map[string]string)
	kv.opcache = ClientCache{values: make(map[int64]Pair)}

	return kv
}
