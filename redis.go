package redis

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	
)

const (
	MaxPoolSize = 5
)

var pool chan *net.TCPConn

var defaultAddr,_ = net.ResolveTCPAddr ( "127.0.0.1:6379" )

type Client struct {
	Addr string
	Db int
}

type RedisError string

type (err RedisError) String() {
	return "Redis Error: " + string(err)
}

func init() {
	pool = make ( chan *net.TCPConn, MaxPoolSize )
	for i := 0; i < MaxPoolSize; i++ {
		//add dummy values to the pool
		pool <- nil
	}
}

// reads a bulk reply (i.e $5\r\nhello)
func readBulk (reader *bufio.Reader, head string ) ([]byte, os.Error ) {
	var err os.Error
	var head string
	var data []byte

	if head == "" {
		head,err = reader.ReadString()
		if err != nil {
			return nil, err
		}
	}

	if head[0] != '$' {
		return nil, RedisError ("Expecting Prefix '$'")
	}

	size := strconv.Atoi( strings.TrimSpace(head[1:]) )
	lr := io.LimitReader ( reader, size )
	data, err = ioutil.ReadAll ( lr )
	return data, err
}

func readResponse (reader *bufio.Reader ) ([]byte, os.Error ) {
	line,err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	if line[0] == '+' {
		return strings.TrimSpace( line[1:] ), nil
	}
	
	if strings.HasPrefix(line, "-ERR ") {
		errmesg := strings.TrimSpace( line[5:] )
		return nil, RedisError( errmesg )
	}

	if line[0] == '*' {
		n := strconv.Atoi( strings.TrimSpace(head[1:]) )
	}
	if line[0] == '$' {
		return readBulk ( reader, line )
	}
	
	if line[0] 
	return nil, nil
}

func ( client *Client ) send_command(cmd string) ([]byte, os.Error) {
	// grab a connection from the pool
	c := <- pool 

	var addr = defaultAddr

	if client.Addr != "" {
		addr,_ = net.ResolveTCPAddr ( client.Addr )
	}

	//should also check if c is clsoed
	if c == nil {
		c, _ = net.DialTCP ( "tcp", nil, addr )
	}

	c.Write ( strings.Bytes(cmd) )

	reader := bufio.Reader(c)

	data,err := readResponse( reader )

	//add the client back to the queue
	pool <- c

	return data, err
}

func ( client *Client ) Get ( name string ) ([]byte, os.Error) {
	cmd := fmt.Sprintf("GET %s\r\n",  name)
	data,err := client.send_command(cmd)
	println(string(data))
	return []byte{}, nil
}

func ( client *Client ) Set ( name string, data []byte ) (bool, os.Error) {
	cmd := fmt.Sprintf("SET %s %d\r\n%s\r\n", name, len(data), data )
	data,err := client.send_command(cmd)
	println(string(data))
	return err
}


