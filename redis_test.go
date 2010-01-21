package redis

import (
    "runtime"
    "strconv"
    "strings"
    "testing"
    //"time"
)

const (
    // the timeout config property in redis.conf. used to test
    // connection retrying
    serverTimeout = 5
)

var client Client

func init() { runtime.GOMAXPROCS(2) }

func TestBasic(t *testing.T) {
    client.Set("a", strings.Bytes("hello"))

    val, err := client.Get("a")

    if err != nil {
        t.Fatal(err.String())
    }

    if string(val) != "hello" {
        t.Fatal("Not equal")
    }
}

func setget(t *testing.T, i int) {
    s := strconv.Itoa(i)
    err := client.Set(s, strings.Bytes(s))
    if err != nil {
        t.Fatal("Concurrent set", err.String())
    }

    s2, err := client.Get(s)

    if err != nil {
        t.Fatal("Concurrent get", err.String())
    }

    if s != string(s2) {
        t.Fatal("Concurrent: value not the same")
    }
}

func TestConcurrent(t *testing.T) {
    for i := 0; i < 20; i++ {
        go setget(t, i)
    }
}

/*
func TestTimeout(t *testing.T) {
    client.Set("a", strings.Bytes("hello world"))

	time.Sleep((serverTimeout+10) * 1e9)
    val, err := client.Get("a")

    if err != nil {
        t.Fatal(err.String())
    }

    println(string(val))
}
*/