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
    "reflect"
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
        n, err := strconv.Atoi64(strings.TrimSpace(line[1:]))
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
        if size <= 0 {
            return make([][]byte, 0), nil
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
    //TODO: handle authentication here

    return
}


func (client *Client) sendCommand(cmd string, args []string) (data interface{}, err os.Error) {
    cmdbuf := bytes.NewBufferString(fmt.Sprintf("*%d\r\n$%d\r\n%s\r\n", len(args)+1, len(cmd), cmd))
    for _, s := range args {
        cmdbuf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
    }

    // grab a connection from the pool
    c := <-pool

    if c == nil {
        c, err = client.openConnection()
        if err != nil {
            goto End
        }
    }
    data, err = client.rawSend(c, cmdbuf.Bytes())
    if err == os.EOF {
        c, err = client.openConnection()
        if err != nil {
            goto End
        }

        data, err = client.rawSend(c, cmdbuf.Bytes())
    }

End:

    //add the client back to the queue
    pool <- c

    return data, err
}

// General Commands

func (client *Client) Auth(password string) os.Error {
    _, err := client.sendCommand("AUTH", []string{password})
    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Exists(key string) (bool, os.Error) {
    cmd := fmt.Sprintf("EXISTS %s\r\n", key)
    res, err := client.sendCommand(cmd, []string{key})
    if err != nil {
        return false, err
    }
    return res.(int64) == 1, nil
}

func (client *Client) Del(key string) (bool, os.Error) {
    res, err := client.sendCommand("DEL", []string{key})

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (client *Client) Type(key string) (string, os.Error) {
    res, err := client.sendCommand("TYPE", []string{key})

    if err != nil {
        return "", err
    }

    return res.(string), nil
}

func (client *Client) Keys(pattern string) ([]string, os.Error) {
    res, err := client.sendCommand("KEYS", []string{pattern})

    if err != nil {
        return nil, err
    }

    var ok bool
    var keydata [][]byte

    if keydata, ok = res.([][]byte); ok {
        // key data is already a double byte array
    } else {
        keydata = bytes.Fields(res.([]byte))
    }
    ret := make([]string, len(keydata))
    for i, k := range keydata {
        ret[i] = string(k)
    }
    return ret, nil
}

func (client *Client) Randomkey() (string, os.Error) {
    res, err := client.sendCommand("RANDOMKEY", nil)
    if err != nil {
        return "", err
    }
    return res.(string), nil
}


func (client *Client) Rename(src string, dst string) os.Error {
    _, err := client.sendCommand("RENAME", []string{src, dst})
    if err != nil {
        return err
    }
    return nil
}

func (client *Client) Renamenx(src string, dst string) (bool, os.Error) {
    res, err := client.sendCommand("RENAMENX", []string{src, dst})
    if err != nil {
        return false, err
    }
    return res.(int64) == 1, nil
}

func (client *Client) Dbsize() (int, os.Error) {
    res, err := client.sendCommand("DBSIZE", nil)
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (client *Client) Expire(key string, time int64) (bool, os.Error) {
    res, err := client.sendCommand("EXPIRE", []string{key, strconv.Itoa64(time)})

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (client *Client) Ttl(key string) (int64, os.Error) {
    res, err := client.sendCommand("TTL", []string{key})
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (client *Client) Move(key string, dbnum int) (bool, os.Error) {
    res, err := client.sendCommand("MOVE", []string{key, strconv.Itoa(dbnum)})

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (client *Client) Flush(all bool) os.Error {
    var cmd string
    if all {
        cmd = "FLUSHALL"
    } else {
        cmd = "FLUSHDB"
    }
    _, err := client.sendCommand(cmd, nil)
    if err != nil {
        return err
    }
    return nil
}

// String-related commands

func (client *Client) Set(key string, val []byte) os.Error {
    _, err := client.sendCommand("SET", []string{key, string(val)})

    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Get(key string) ([]byte, os.Error) {
    res, _ := client.sendCommand("GET", []string{key})

    if res == nil {
        return nil, RedisError("Key `" + key + "` does not exist")
    }

    data := res.([]byte)
    return data, nil
}

func (client *Client) Getset(key string, val []byte) ([]byte, os.Error) {
    res, err := client.sendCommand("GETSET", []string{key, string(val)})

    if err != nil {
        return nil, err
    }

    data := res.([]byte)
    return data, nil
}

func (client *Client) Mget(keys []string) ([][]byte, os.Error) {
    res, err := client.sendCommand("MGET", keys)
    if err != nil {
        return nil, err
    }

    data := res.([][]byte)
    return data, nil
}

func (client *Client) Setnx(key string, val []byte) os.Error {
    _, err := client.sendCommand("SETNX", []string{key, string(val)})

    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Setex(key string, time int64, val []byte) os.Error {
    _, err := client.sendCommand("SETEX", []string{key, strconv.Itoa64(time), string(val)})

    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Mset(mapping map[string][]byte) os.Error {
    args := make([]string, len(mapping)*2)
    i := 0
    for k, v := range mapping {
        args[i] = k
        args[i+1] = string(v)
        i += 2
    }
    _, err := client.sendCommand("MSET", args)
    if err != nil {
        return err
    }
    return nil
}

func (client *Client) Msetnx(mapping map[string][]byte) os.Error {
    args := make([]string, len(mapping)*2)
    i := 0
    for k, v := range mapping {
        args[i] = k
        args[i+1] = string(v)
        i += 2
    }
    _, err := client.sendCommand("MSETNX", args)
    if err != nil {
        return err
    }
    return nil
}

func (client *Client) Incr(key string) (int64, os.Error) {
    res, err := client.sendCommand("INCR", []string{key})
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (client *Client) Incrby(key string, val int64) (int64, os.Error) {
    res, err := client.sendCommand("INCRBY", []string{key, strconv.Itoa64(val)})
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (client *Client) Decr(key string) (int64, os.Error) {
    res, err := client.sendCommand("DECR", []string{key})
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (client *Client) Decrby(key string, val int64) (int64, os.Error) {
    res, err := client.sendCommand("DECRBY", []string{key, strconv.Itoa64(val)})
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (client *Client) Append(key string, val []byte) os.Error {
    _, err := client.sendCommand("APPEND", []string{key, string(val)})

    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Substr(key string, start int, end int) ([]byte, os.Error) {
    res, _ := client.sendCommand("SUBSTR", []string{key, strconv.Itoa(start), strconv.Itoa(end)})

    if res == nil {
        return nil, RedisError("Key `" + key + "` does not exist")
    }

    data := res.([]byte)
    return data, nil
}

// List commands

func (client *Client) Rpush(key string, val []byte) os.Error {
    _, err := client.sendCommand("RPUSH", []string{key, string(val)})

    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Lpush(key string, val []byte) os.Error {
    _, err := client.sendCommand("LPUSH", []string{key, string(val)})

    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Llen(key string) (int, os.Error) {
    res, err := client.sendCommand("LLEN", []string{key})
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (client *Client) Lrange(key string, start int, end int) ([][]byte, os.Error) {
    res, err := client.sendCommand("LRANGE", []string{key, strconv.Itoa(start), strconv.Itoa(end)})
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (client *Client) Ltrim(key string, start int, end int) os.Error {
    _, err := client.sendCommand("LTRIM", []string{key, strconv.Itoa(start), strconv.Itoa(end)})

    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Lindex(key string, index int) ([]byte, os.Error) {
    res, err := client.sendCommand("LINDEX", []string{key, strconv.Itoa(index)})

    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (client *Client) Lset(key string, index int, value []byte) os.Error {
    _, err := client.sendCommand("LSET", []string{key, strconv.Itoa(index), string(value)})
    if err != nil {
        return err
    }

    return nil
}

func (client *Client) Lrem(key string, index int) (int, os.Error) {
    res, err := client.sendCommand("LREM", []string{key, strconv.Itoa(index)})
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (client *Client) Lpop(key string) ([]byte, os.Error) {
    res, err := client.sendCommand("LPOP", []string{key})
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (client *Client) Rpop(key string) ([]byte, os.Error) {
    res, err := client.sendCommand("RPOP", []string{key})
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (client *Client) Blpop(key string) ([]byte, os.Error) {
    res, err := client.sendCommand("BLPOP", []string{key})
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (client *Client) Brpop(key string) ([]byte, os.Error) {
    res, err := client.sendCommand("BRPOP", []string{key})
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

func (client *Client) Rpoplpush(src string, dst string) ([]byte, os.Error) {
    res, err := client.sendCommand("RPOPLPUSH", []string{src, dst})
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

// Set commands

func (client *Client) Sadd(key string, value []byte) (bool, os.Error) {
    res, err := client.sendCommand("SADD", []string{key, string(value)})

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (client *Client) Srem(key string, value []byte) (bool, os.Error) {
    res, err := client.sendCommand("SREM", []string{key, string(value)})

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (client *Client) Spop(key string) ([]byte, os.Error) {
    res, err := client.sendCommand("SPOP", []string{key})
    if err != nil {
        return nil, err
    }

    if res == nil {
        return nil, RedisError("Spop failed")
    }

    data := res.([]byte)
    return data, nil
}

func (client *Client) Smove(src string, dst string, val []byte) (bool, os.Error) {
    res, err := client.sendCommand("SMOVE", []string{src, dst, string(val)})
    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (client *Client) Scard(key string) (int, os.Error) {
    res, err := client.sendCommand("SCARD", []string{key})
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (client *Client) Sismember(key string, value []byte) (bool, os.Error) {
    res, err := client.sendCommand("SISMEMBER", []string{key, string(value)})

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (client *Client) Sinter(keys []string) ([][]byte, os.Error) {
    res, err := client.sendCommand("SINTER", keys)
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (client *Client) Sinterstore(dst string, keys []string) (int, os.Error) {
    args := make([]string, len(keys)+1)
    args[0] = dst
    copy(args[1:], keys)
    res, err := client.sendCommand("SINTERSTORE", args)
    if err != nil {
        return 0, err
    }

    return int(res.(int64)), nil
}

func (client *Client) Sunion(keys []string) ([][]byte, os.Error) {
    res, err := client.sendCommand("SUNION", keys)
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (client *Client) Sunionstore(dst string, keys []string) (int, os.Error) {
    args := make([]string, len(keys)+1)
    args[0] = dst
    copy(args[1:], keys)
    res, err := client.sendCommand("SUNIONSTORE", args)
    if err != nil {
        return 0, err
    }

    return int(res.(int64)), nil
}

func (client *Client) Sdiff(key1 string, keys []string) ([][]byte, os.Error) {
    args := make([]string, len(keys)+1)
    args[0] = key1
    copy(args[1:], keys)
    res, err := client.sendCommand("SDIFF", args)
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (client *Client) Sdiffstore(dst string, key1 string, keys []string) (int, os.Error) {
    args := make([]string, len(keys)+2)
    args[0] = dst
    args[1] = key1
    copy(args[2:], keys)
    res, err := client.sendCommand("SDIFFSTORE", args)
    if err != nil {
        return 0, err
    }

    return int(res.(int64)), nil
}

func (client *Client) Smembers(key string) ([][]byte, os.Error) {
    res, err := client.sendCommand("SMEMBERS", []string{key})

    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (client *Client) Srandmember(key string) ([]byte, os.Error) {
    res, err := client.sendCommand("SRANDMEMBER", []string{key})
    if err != nil {
        return nil, err
    }

    return res.([]byte), nil
}

// sorted set commands

func (client *Client) Zadd(key string, value []byte, score float64) (bool, os.Error) {
    res, err := client.sendCommand("ZADD", []string{key, string(value), strconv.Ftoa64(score, 'f', -1)})
    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (client *Client) Zrem(key string, value []byte) (bool, os.Error) {
    res, err := client.sendCommand("ZREM", []string{key, string(value)})
    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (client *Client) Zincrby(key string, value []byte, score float64) (float64, os.Error) {
    res, err := client.sendCommand("ZINCRBY", []string{key, string(value), strconv.Ftoa64(score, 'f', -1)})
    if err != nil {
        return 0, err
    }

    data := string(res.([]byte))
    f, _ := strconv.Atof64(data)
    return f, nil
}

func (client *Client) Zrank(key string, value []byte) (int, os.Error) {
    res, err := client.sendCommand("ZRANK", []string{key, string(value)})
    if err != nil {
        return 0, err
    }

    return int(res.(int64)), nil
}

func (client *Client) Zrevrank(key string, value []byte) (int, os.Error) {
    res, err := client.sendCommand("ZREVRANK", []string{key, string(value)})
    if err != nil {
        return 0, err
    }

    return int(res.(int64)), nil
}

func (client *Client) Zrange(key string, start int, end int) ([][]byte, os.Error) {
    res, err := client.sendCommand("ZRANGE", []string{key, strconv.Itoa(start), strconv.Itoa(end)})
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (client *Client) Zrevrange(key string, start int, end int) ([][]byte, os.Error) {
    res, err := client.sendCommand("ZREVRANGE", []string{key, strconv.Itoa(start), strconv.Itoa(end)})
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (client *Client) Zrangebyscore(key string, start float64, end float64) ([][]byte, os.Error) {
    res, err := client.sendCommand("ZRANGEBYSCORE", []string{key, strconv.Ftoa64(start, 'f', -1), strconv.Ftoa64(end, 'f', -1)})
    if err != nil {
        return nil, err
    }

    return res.([][]byte), nil
}

func (client *Client) Zcard(key string) (int, os.Error) {
    res, err := client.sendCommand("ZCARD", []string{key})
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (client *Client) Zscore(key string) (float64, os.Error) {
    res, err := client.sendCommand("ZSCORE", []string{key})
    if err != nil {
        return 0, err
    }

    data := string(res.([]byte))
    f, _ := strconv.Atof64(data)
    return f, nil
}

func (client *Client) Zremrangebyrank(key string, start int, end int) (int, os.Error) {
    res, err := client.sendCommand("ZREMRANGEBYRANK", []string{key, strconv.Itoa(start), strconv.Itoa(end)})
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (client *Client) Zremrangebyscore(key string, start float64, end float64) (int, os.Error) {
    res, err := client.sendCommand("ZREMRANGEBYSCORE", []string{key, strconv.Ftoa64(start, 'f', -1), strconv.Ftoa64(end, 'f', -1)})
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

// hash commands

func (client *Client) Hset(key string, field string, val []byte) (bool, os.Error) {
    res, err := client.sendCommand("HSET", []string{key, field, string(val)})
    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (client *Client) Hget(key string, field string) ([]byte, os.Error) {
    res, _ := client.sendCommand("HGET", []string{key, field})

    if res == nil {
        return nil, RedisError("Hget failed")
    }

    data := res.([]byte)
    return data, nil
}

//pretty much copy the json code from here.

func valueToString(v reflect.Value) (string, os.Error) {
    if v == nil {
        return "null", nil
    }

    switch v := v.(type) {
    case *reflect.BoolValue:
        x := v.Get()
        if x {
            return "true", nil
        } else {
            return "false", nil
        }

    case *reflect.IntValue:
        return strconv.Itoa(v.Get()), nil
    case *reflect.Int8Value:
        return strconv.Itoa(int(v.Get())), nil
    case *reflect.Int16Value:
        return strconv.Itoa(int(v.Get())), nil
    case *reflect.Int32Value:
        return strconv.Itoa(int(v.Get())), nil
    case *reflect.Int64Value:
        return strconv.Itoa64(v.Get()), nil

    case *reflect.UintValue:
        return strconv.Uitoa(v.Get()), nil
    case *reflect.Uint8Value:
        return strconv.Uitoa(uint(v.Get())), nil
    case *reflect.Uint16Value:
        return strconv.Uitoa(uint(v.Get())), nil
    case *reflect.Uint32Value:
        return strconv.Uitoa(uint(v.Get())), nil
    case *reflect.Uint64Value:
        return strconv.Uitoa64(v.Get()), nil
    case *reflect.UintptrValue:
        return strconv.Uitoa64(uint64(v.Get())), nil

    case *reflect.FloatValue:
        return strconv.Ftoa(v.Get(), 'g', -1), nil
    case *reflect.Float32Value:
        return strconv.Ftoa32(v.Get(), 'g', -1), nil
    case *reflect.Float64Value:
        return strconv.Ftoa64(v.Get(), 'g', -1), nil

    case *reflect.StringValue:
        return v.Get(), nil
    case *reflect.SliceValue:
        typ := v.Type().(*reflect.SliceType)
        if _, ok := typ.Elem().(*reflect.Uint8Type); ok {
            return string(v.Interface().([]byte)), nil
        }
    }
    return "", os.NewError("Unsupported type")
}

func containerToString(val reflect.Value, args *vector.StringVector) os.Error {
    switch v := val.(type) {
    case *reflect.PtrValue:
        return containerToString(reflect.Indirect(v), args)
    case *reflect.InterfaceValue:
        return containerToString(v.Elem(), args)
    case *reflect.MapValue:
        if _, ok := v.Type().(*reflect.MapType).Key().(*reflect.StringType); !ok {
            return os.NewError("Unsupported type - map key must be a string")
        }
        for _, k := range v.Keys() {
            args.Push(k.(*reflect.StringValue).Get())
            s, err := valueToString(v.Elem(k))
            if err != nil {
                return err
            }
            args.Push(s)
        }
    case *reflect.StructValue:
        st := v.Type().(*reflect.StructType)
        for i := 0; i < st.NumField(); i++ {
            ft := st.FieldByIndex([]int{i})
            args.Push(ft.Name)
            s, err := valueToString(v.FieldByIndex([]int{i}))
            if err != nil {
                return err
            }
            args.Push(s)
        }
    }
    return nil
}

func (client *Client) Hmset(key string, mapping interface{}) os.Error {
    args := new(vector.StringVector)
    args.Push(key)
    err := containerToString(reflect.NewValue(mapping), args)
    if err != nil {
        return err
    }
    _, err = client.sendCommand("HMSET", *args)
    if err != nil {
        return err
    }
    return nil
}

func (client *Client) Hincrby(key string, field string, val int64) (int64, os.Error) {
    res, err := client.sendCommand("HINCRBY", []string{key, field, strconv.Itoa64(val)})
    if err != nil {
        return -1, err
    }

    return res.(int64), nil
}

func (client *Client) Hexists(key string, field string) (bool, os.Error) {
    res, err := client.sendCommand("HEXISTS", []string{key, field})
    if err != nil {
        return false, err
    }
    return res.(int64) == 1, nil
}

func (client *Client) Hdel(key string, field string) (bool, os.Error) {
    res, err := client.sendCommand("HDEL", []string{key, field})

    if err != nil {
        return false, err
    }

    return res.(int64) == 1, nil
}

func (client *Client) Hlen(key string) (int, os.Error) {
    res, err := client.sendCommand("HLEN", []string{key})
    if err != nil {
        return -1, err
    }

    return int(res.(int64)), nil
}

func (client *Client) Hkeys(key string) ([]string, os.Error) {
    res, err := client.sendCommand("HKEYS", []string{key})

    if err != nil {
        return nil, err
    }

    data := res.([][]byte)
    ret := make([]string, len(data))
    for i, k := range data {
        ret[i] = string(k)
    }
    return ret, nil
}

func (client *Client) Hvals(key string) ([][]byte, os.Error) {
    res, err := client.sendCommand("HVALS", []string{key})

    if err != nil {
        return nil, err
    }
    return res.([][]byte), nil
}

func writeTo(data []byte, val reflect.Value) os.Error {
    s := string(data)
    switch v := val.(type) {
    case *reflect.BoolValue:
        b, err := strconv.Atob(s)
        if err != nil {
            return err
        }
        v.Set(b)
    case *reflect.IntValue:
        i, err := strconv.Atoi(s)
        if err != nil {
            return err
        }
        v.Set(i)
    case *reflect.Int8Value:
        i, err := strconv.Atoi(s)
        if err != nil {
            return err
        }
        v.Set(int8(i))
    case *reflect.Int16Value:
        i, err := strconv.Atoi(s)
        if err != nil {
            return err
        }
        v.Set(int16(i))
    case *reflect.Int32Value:
        i, err := strconv.Atoi(s)
        if err != nil {
            return err
        }
        v.Set(int32(i))
    case *reflect.Int64Value:
        i, err := strconv.Atoi64(s)
        if err != nil {
            return err
        }
        v.Set(i)
    case *reflect.UintValue:
        ui, err := strconv.Atoui(s)
        if err != nil {
            return err
        }
        v.Set(ui)

    case *reflect.Uint8Value:
        ui, err := strconv.Atoui(s)
        if err != nil {
            return err
        }
        v.Set(uint8(ui))
    case *reflect.Uint16Value:
        ui, err := strconv.Atoui(s)
        if err != nil {
            return err
        }
        v.Set(uint16(ui))
    case *reflect.Uint32Value:
        ui, err := strconv.Atoui(s)
        if err != nil {
            return err
        }
        v.Set(uint32(ui))
    case *reflect.Uint64Value:
        ui, err := strconv.Atoui64(s)
        if err != nil {
            return err
        }
        v.Set(ui)
    case *reflect.UintptrValue:
        ui, err := strconv.Atoui64(s)
        if err != nil {
            return err
        }
        v.Set(uintptr(ui))
    case *reflect.FloatValue:
        f, err := strconv.Atof(s)
        if err != nil {
            return err
        }
        v.Set(f)

    case *reflect.Float32Value:
        f, err := strconv.Atof32(s)
        if err != nil {
            return err
        }
        v.Set(f)
    case *reflect.Float64Value:
        f, err := strconv.Atof64(s)
        if err != nil {
            return err
        }
        v.Set(f)

    case *reflect.StringValue:
        v.Set(s)
    case *reflect.SliceValue:
        typ := v.Type().(*reflect.SliceType)
        if _, ok := typ.Elem().(*reflect.Uint8Type); ok {
            v.Set(reflect.NewValue(data).(*reflect.SliceValue))
        }
    }
    return nil
}

func writeToContainer(data [][]byte, val reflect.Value) os.Error {
    switch v := val.(type) {
    case *reflect.PtrValue:
        return writeToContainer(data, reflect.Indirect(v))
    case *reflect.InterfaceValue:
        return writeToContainer(data, v.Elem())
    case *reflect.MapValue:
        if _, ok := v.Type().(*reflect.MapType).Key().(*reflect.StringType); !ok {
            return os.NewError("Invalid map type")
        }
        elemtype := v.Type().(*reflect.MapType).Elem()
        for i := 0; i < len(data)/2; i++ {
            mk := reflect.NewValue(string(data[i*2]))
            mv := reflect.MakeZero(elemtype)
            writeTo(data[i*2+1], mv)
            v.SetElem(mk, mv)
        }
    case *reflect.StructValue:
        for i := 0; i < len(data)/2; i++ {
            name := string(data[i*2])
            field := v.FieldByName(name)
            if field == nil {
                continue
            }
            writeTo(data[i*2+1], field)
        }
    default:
        return os.NewError("Invalid container type")
    }
    return nil
}


func (client *Client) Hgetall(key string, val interface{}) os.Error {
    res, err := client.sendCommand("HGETALL", []string{key})
    if err != nil {
        return err
    }
    data := res.([][]byte)
    err = writeToContainer(data, reflect.NewValue(val))
    if err != nil {
        return err
    }

    return nil
}

//Server commands

func (client *Client) Save() os.Error {
    _, err := client.sendCommand("SAVE", nil)
    if err != nil {
        return err
    }
    return nil
}

func (client *Client) Bgsave() os.Error {
    _, err := client.sendCommand("BGSAVE", nil)
    if err != nil {
        return err
    }
    return nil
}

func (client *Client) Lastsave() (int64, os.Error) {
    res, err := client.sendCommand("LASTSAVE", nil)
    if err != nil {
        return 0, err
    }

    return res.(int64), nil
}

func (client *Client) Bgrewriteaof() os.Error {
    _, err := client.sendCommand("BGREWRITEAOF", nil)
    if err != nil {
        return err
    }
    return nil
}
