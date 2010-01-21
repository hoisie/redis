package redis

import (
    "strings"
    "testing"
)

func TestBasic(t *testing.T) {
    println("Testing basic")

    var client Client
    client.Set("a", strings.Bytes("hello world"))

    val, err := client.Get("a")

    if err != nil {
        println(err.String())
    }

    println(string(val))
}
