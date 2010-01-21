package main

import "bufio"
import "net"
import "os"
import "strings"
//import "redis"

func load_db(reader *bufio.Reader) {
    c, _ := net.Dial("tcp", "", "127.0.0.1:6379")
    for {
        line, err := reader.ReadBytes('\n')
        if err == os.EOF {
            break
        }
        println(string(line))
        c.Write(line)
    }
    c.Write(strings.Bytes("QUIT\r\n"))
    buf := make([]byte, 512)

    for {
        n, err := c.Read(buf)
        if err != nil {
            break
        }
        println(string(buf[0:n]))
    }
}

func main() { load_db(bufio.NewReader(os.Stdin)) }
