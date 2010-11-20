## redis.go

redis.go is a client for the [redis](http://github.com/antirez/redis) key-value store. 

Some features include:

* Designed for Redis 1.3.x. 
* Support for all redis types - strings, lists, sets, sorted sets, and hashes
* Very simple usage
* Connection pooling ( with configurable size )
* Support for concurrent access
* Manages connections to the redis server, including dropped and timed out connections
* Marshaling/Unmarshaling go types to hashes

This library is stable and is used in production environments. However, some commands have not been tested as thoroughly as others. If you find any bugs please file an issue!

## Examples

Most of the examples connect to a redis database running in the default port -- 6367. 

### Instantiating the client

    //connects to the default port (6379)
    var client redis.Client 
     
    //connects to port 8379, database 13
    var client2 redis.Client
    client2.Addr = "127.0.0.1:8379"
    client2.Db = 13

### Strings 

    var client redis.Client
    client.Set("a", []byte("hello"))
    val, _ := client.Get("a")
    println(string(val))
    client.Del("a")

### Lists

    var client redis.Client
    vals := []string{"a", "b", "c", "d", "e"}
    for _, v := range vals {
        client.Rpush("l", []byte(v))
    }
    dbvals,_ := client.Lrange("l", 0, 4)
    for i, v := range dbvals {
        println(i,":",string(v))
    }
    client.Del("l")

### Publish/Subscribe
    sub := make(chan string, 1)
    sub <- "foo"
    messages := make(chan Message, 0)
    go client.Subscribe(sub, nil, nil, nil, messages)

    time.Sleep(10 * 1000 * 1000)
    client.Publish("foo", []byte("bar"))

    msg := <-messages
    println("received from:", msg.Channel, " message:", string(msg.Message))

    close(sub)
    close(messages)


More examples coming soon. See `redis_test.go` for more usage examples.

## Commands not supported yet

* MULTI/EXEC/DISCARD/WATCH/UNWATCH
* SORT
* ZUNIONSTORE / ZINTERSTORE

