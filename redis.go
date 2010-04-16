package redis

import (
    "bufio"
    "bytes"
    "container/vector"
    "fmt"
    "io"
    "io/ioutil"
    "net"
    "os"
    "strconv"
    "strings"
)

const (
    MaxPoolSize = 5
)

var pool chan *net.TCPConn

var defaultAddr, _ = net.ResolveTCPAddr("127.0.0.1:6379")

type Client struct {
    Addr     string
    Db       int
    Password string
}

type RedisError string

func (err RedisError) String() string { return "Redis Error: " + string(err) }

func init() {
    pool = make(chan *net.TCPConn, MaxPoolSize)
    for i := 0; i < MaxPoolSize; i++ {
        //add dummy values to the pool
        pool <- nil
    }
}

// reads a bulk reply (i.e $5\r\nhello)
func readBulk(reader *bufio.Reader, head string) ([]byte, os.Error) {
    var err os.Error
    var data []byte

    if head == "" {
        head, err = reader.ReadString('\n')
        if err != nil {
            return nil, err
        }
    }

    if head[0] != '$' {
        return nil, RedisError("Expecting Prefix '$'")
    }

    size, err := strconv.Atoi(strings.TrimSpace(head[1:]))

    if size == -1 {
        return nil, RedisError("Key does not exist ")
    }
    lr := io.LimitReader(reader, int64(size))
    data, err = ioutil.ReadAll(lr)

    return data, err
}

func readResponse(reader *bufio.Reader) (interface{}, os.Error) {

    var line string
    var err os.Error

    //read until the first non-whitespace line
    for {
        line, err = reader.ReadString('\n')
        if len(line) == 0 || err != nil {
            return nil, err
        }
        line = strings.TrimSpace(line)
        if len(line) > 0 {
            break
        }
    }

    if line[0] == '+' {
        return strings.TrimSpace(line[1:]), nil
    }

    if strings.HasPrefix(line, "-ERR ") {
        errmesg := strings.TrimSpace(line[5:])
        return nil, RedisError(errmesg)
    }

    if line[0] == ':' {
        n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
        if err != nil {
            return nil, RedisError("Int reply is not a number")
        }
        return n, nil
    }

    if line[0] == '*' {
        size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
        if err != nil {
            return nil, RedisError("MultiBulk reply expected a number")
        }
        res := make([][]byte, size)
        for i := 0; i < size; i++ {
            res[i], err = readBulk(reader, "")
            if err != nil {
                return nil, err
            }
            //read the end line
            _, err = reader.ReadString('\n')
            if err != nil {
                return nil, err
            }
        }
        return res, nil
    }

    return readBulk(reader, line)
}

func (client *Client) rawSend(c *net.TCPConn, cmd []byte) (interface{}, os.Error) {

    _, err := c.Write(cmd)

    if err != nil {
        return nil, err
    }

    reader := bufio.NewReader(c)

    data, err := readResponse(reader)
    if err != nil {
        return nil, err
    }

    return data, nil
}

func (client *Client) openConnection() (c *net.TCPConn, err os.Error) {

    var addr = defaultAddr

    if client.Addr != "" {
        addr, err = net.ResolveTCPAddr(client.Addr)

        if err != nil {
            return
        }

    }

    c, err = net.DialTCP("tcp", nil, addr)

    if err != nil {
        return
    }

    if client.Db != 0 {
        cmd := fmt.Sprintf("SELECT %d\r\n", client.Db)
        _, err = client.rawSend(c, []byte(cmd))
        if err != nil {
            return
        }
    }

    return
}


func (client *Client) sendCommand(cmd string) (data interface{}, err os.Error) {
    // grab a connection from the pool
    c := <-pool

    if c == nil {
        c, err = client.openConnection()
        if err != nil {
            goto End
        }
    }

    data, err = client.rawSend(c, []byte(cmd))

    if err == os.EOF {
        c, err = client.openConnection()
        if err != nil {
            goto End
        }

        data, err = client.rawSend(c, []byte(cmd))
    }

End:

    //add the client back to the queue
    pool <- c

    return data, err
}

func (client *Client) Get(name string) ([]byte, os.Error) {
    cmd := fmt.Sprintf("GET %s\r\n", name)
    res, _ := client.sendCommand(cmd)

    if res == nil {
        return nil, RedisError("Key `" + name + "` does not exist")
    }

    data := res.([]byte)
    return data, nil
}

func (client *Client) Set(key string, val []byte) os.Error {
    cmd := fmt.Sprintf("SET %s %d\r\n%s\r\n", key, len(val), val)
    _, err := client.sendCommand(cmd)

    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Del(name string) (bool, os.Error) {
    cmd := fmt.Sprintf("DEL %s\r\n", name)
    res, err := client.sendCommand(cmd)

    if err != nil {
        return false, err
    }

    return res.(int) == 1, nil
}

func (client *Client) Keys(pattern string) ([]string, os.Error) {
    cmd := fmt.Sprintf("KEYS %s\r\n", pattern)
    res, err := client.sendCommand(cmd)

    if err != nil {
        return nil, err
    }

    keys := bytes.Fields(res.([]byte))
    var ret vector.StringVector
    for _, k := range keys {
        ret.Push(string(k))
    }
    return ret, nil
}

func (client *Client) Type(key string) (string, os.Error) {
    cmd := fmt.Sprintf("TYPE %s\r\n", key)
    res, err := client.sendCommand(cmd)

    if err != nil {
        return "", err
    }

    return res.(string), nil
}

func (client *Client) Auth(password string) os.Error {
    cmd := fmt.Sprintf("AUTH %s\r\n", password)
    _, err := client.sendCommand(cmd)
    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Exists(key string) (bool, os.Error) {
    cmd := fmt.Sprintf("EXISTS %s\r\n", key)
    res, err := client.sendCommand(cmd)
    if err != nil {
        return false, err
    }
    return res.(int) == 1, nil
}

func (client *Client) Rename(src string, dst string) os.Error {
    cmd := fmt.Sprintf("RENAME %s %s\r\n", src, dst)

    _, err := client.sendCommand(cmd)
    if err != nil {
        return err
    }
    return nil
}

func (client *Client) Renamenx(src string, dst string) (bool, os.Error) {
    cmd := fmt.Sprintf("RENAMENX %s %s\r\n", src, dst)

    res, err := client.sendCommand(cmd)
    if err != nil {
        return false, err
    }
    return res.(int) == 1, nil
}

func (client *Client) Randomkey() (string, os.Error) {
    res, err := client.sendCommand("RANDOMKEY\r\n")
    if err != nil {
        return "", err
    }
    return res.(string), nil
}

func (client *Client) Dbsize() (int64, os.Error) {
    res, err := client.sendCommand("DBSIZE\r\n")
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (client *Client) Expire(key string, time int64) (bool, os.Error) {
    cmd := fmt.Sprintf("EXPIRE %s %d\r\n", key, time)
    res, err := client.sendCommand(cmd)

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (client *Client) Ttl(key string) (int64, os.Error) {
    cmd := fmt.Sprintf("TTL %s\r\n", key)
    res, err := client.sendCommand(cmd)
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (client *Client) Move(key string, dbnum int) (bool, os.Error) {
    cmd := fmt.Sprintf("MOVE %s %d\r\n", key, dbnum)
    res, err := client.sendCommand(cmd)

    if err != nil {
        return false, err
    }

    return res.(int) == 1, nil
}

func (client *Client) Flush(all bool) os.Error {
    var cmd string
    if all {
        cmd = "FLUSHALL\r\n"
    } else {
        cmd = "FLUSHDB\r\n"
    }
    _, err := client.sendCommand(cmd)
    if err != nil {
        return err
    }
    return nil
}

func (client *Client) Llen(name string) (int, os.Error) {
    cmd := fmt.Sprintf("LLEN %s\r\n", name)
    res, err := client.sendCommand(cmd)
    if err != nil {
        return -1, err
    }

    return res.(int), nil
}

func (client *Client) Lrange(name string, start int, end int) ([][]byte, os.Error) {
    cmd := fmt.Sprintf("LRANGE %d %d\r\n", start, end)
    res, err := client.sendCommand(cmd)
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (client *Client) Lindex(name string, index int) ([]byte, os.Error) {
    cmd := fmt.Sprintf("LINDEX %s %d\r\n", name, index)
    res, err := client.sendCommand(cmd)

    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (client *Client) Lset(name string, index int, value []byte) os.Error {
    cmd := fmt.Sprintf("LSET %s %d %d\r\n%s\r\n", name, index, len(value), value)
    _, err := client.sendCommand(cmd)
    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Lrem(name string, index int) (int, os.Error) {
    cmd := fmt.Sprintf("LREM %s %d\r\n", name, index)
    res, err := client.sendCommand(cmd)
    if err != nil {
        return -1, err
    }

    return res.(int), nil
}

func (client *Client) Lpop(name string) ([]byte, os.Error) {
    cmd := fmt.Sprintf("LPOP %s\r\n", name)
    res, err := client.sendCommand(cmd)
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (client *Client) Rpop(name string) ([]byte, os.Error) {
    cmd := fmt.Sprintf("RPOP %s\r\n", name)
    res, err := client.sendCommand(cmd)
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (client *Client) Blpop(name string) ([]byte, os.Error) {
    cmd := fmt.Sprintf("BLPOP %s\r\n", name)
    res, err := client.sendCommand(cmd)
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (client *Client) Brpop(name string) ([]byte, os.Error) {
    cmd := fmt.Sprintf("BRPOP %s\r\n", name)
    res, err := client.sendCommand(cmd)
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (client *Client) Rpoplpush(src string, dst string) ([]byte, os.Error) {
    cmd := fmt.Sprintf("RPOPLPUSH %s %s\r\n", src, dst)
    res, err := client.sendCommand(cmd)
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (client *Client) Rpush(name string, value []byte) os.Error {
    cmd := fmt.Sprintf("RPUSH %s %d\r\n%s\r\n", name, len(value), value)
    _, err := client.sendCommand(cmd)

    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Lpush(name string, value []byte) os.Error {
    cmd := fmt.Sprintf("LPUSH %s %d\r\n%s\r\n", name, len(value), value)
    _, err := client.sendCommand(cmd)

    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Ltrim(name string, start int64, end int64) os.Error {
    cmd := fmt.Sprintf("LTRIM %s %d %d\r\n", name, start, end)
    _, err := client.sendCommand(cmd)

    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Sismember(name string, value []byte) (bool, os.Error) {
    cmd := fmt.Sprintf("SISMEMBER %s %d\r\n%s\r\n", name, len(value), value)
    res, err := client.sendCommand(cmd)

    if err != nil {
        return false, err
    }

    return res.(int) == 1, nil
}

func (client *Client) Smembers(name string) ([][]byte, os.Error) {
    cmd := fmt.Sprintf("SMEMBERS %s\r\n", name)
    res, err := client.sendCommand(cmd)

    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (client *Client) Sadd(name string, value []byte) (bool, os.Error) {
    cmd := fmt.Sprintf("SADD %s %d\r\n%s\r\n", name, len(value), value)
    res, err := client.sendCommand(cmd)

    if err != nil {
        return false, err
    }

    return res.(int) == 1, nil
}

func (client *Client) Srem(name string, value []byte) (bool, os.Error) {
    cmd := fmt.Sprintf("SREM %s %d\r\n%s\r\n", name, len(value), value)
    res, err := client.sendCommand(cmd)

    if err != nil {
        return false, err
    }

    return res.(int) == 1, nil
}
