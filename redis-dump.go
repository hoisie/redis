package main

import (
    "fmt"
    "io"
    "os"
    "redis"
)

func dump_db(output io.Writer) {
    var client redis.Client
    keys, err := client.Keys("*")

    if err != nil {
        println("Redis-dump failed", err.String())
        return
    }

    fmt.Fprintf(output, "FLUSHDB\r\n")

    for _, key := range (keys) {
        typ, _ := client.Type(key)

        if typ == "string" {
            data, _ := client.Get(key)
            fmt.Fprintf(output, "SET %s %d\r\n%s\r\n", key, len(data), data)
        } else if typ == "list" {
            llen, _ := client.Llen(key)
            for i := 0; i < llen; i++ {
                data, _ := client.Lindex(key, i)
                fmt.Fprintf(output, "RPUSH %s %d\r\n%s\r\n", key, len(data), data)
            }
        } else if typ == "set" {
            members, _ := client.Smembers(key)
            for _, data := range (members) {
                fmt.Fprintf(output, "SADD %s %d\r\n%s\r\n", key, len(data), data)
            }
        }
    }

}

func main() { dump_db(os.Stdout) }

/*
   for k in keys:
           typ = dsp.get_type(k)
           if typ == 'string':
                   v = dsp[k]
                   fileobj.write("SET %s %s\r\n%s\r\n"%(k,len(v), v))
           elif typ == 'list':
                   l = dsp.llen(k)
                   for i in range(l):
                           v = dsp.lindex(k, i)
                           fileobj.write("RPUSH %s %s\r\n%s\r\n"%(k,len(v), v))
           elif typ == 'set':
                   members = dsp.smembers(k)
                   for m in members:
                           m = str(m)
                           fileobj.write("SADD %s %s\r\n%s\r\n"%(k, len(m), m))

*/