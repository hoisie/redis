## redis.go

redis.go is a client for the [redis](http://github.com/antirez/redis) key-value store. 

Some features include:

* simple usage
* connection pooling ( with configurable size )
* support for concurrent access
* re-opens timed out connections (and re-sends) commands

This library was designed to be robust to concurrency conflicts. It lets you declare one redis client which can be shared amongst all goroutines.  

## example

    //this assumes you have redis running locally on the standard port
    var client redis.Client
    client.Set("a", strings.Bytes("hello"))
    val, _ := client.Get("a")
    println(string(val))


See the test file for more examples :)

