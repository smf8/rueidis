package rueidis

import (
	"context"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func parallel(p int) (chan func(), func()) {
	ch := make(chan func(), p)
	wg := sync.WaitGroup{}
	wg.Add(p)
	for i := 0; i < p; i++ {
		go func() {
			for fn := range ch {
				fn()
			}
			wg.Done()
		}()
	}
	return ch, func() {
		close(ch)
		wg.Wait()
	}
}

func testFlush(t *testing.T, client Client) {
	ctx := context.Background()

	keys := 1000
	para := 8

	kvs := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		kvs[strconv.Itoa(i)] = strconv.FormatInt(rand.Int63(), 10)
	}

	t.Logf("prepare %d keys for FLUSH\n", keys)
	jobs, wait := parallel(para)
	for i := 0; i < keys; i++ {
		key := strconv.Itoa(i)
		jobs <- func() {
			val, err := client.Do(ctx, client.B().Set().Key(key).Value(kvs[key]).Build()).ToString()
			if err != nil || val != "OK" {
				t.Fatalf("unexpected set response %v %v", val, err)
			}
		}
	}
	wait()

	t.Logf("testing client side caching before flush\n")
	jobs, wait = parallel(para)
	for i := 0; i < keys; i++ {
		key := strconv.Itoa(i)
		jobs <- func() {
			resp := client.DoCache(ctx, client.B().Get().Key(key).Cache(), time.Minute)
			val, err := resp.ToString()
			if val != kvs[key] {
				t.Fatalf("unexpected csc get response %v %v", val, err)
			}
			if resp.IsCacheHit() {
				t.Fatalf("unexpected csc cache hit")
			}
		}
	}
	wait()

	if err := client.Do(ctx, client.B().Flushall().Build()).Error(); err != nil {
		t.Fatalf("unexpected flush err %v", err)
	}

	time.Sleep(time.Second)

	t.Logf("testing client side caching after flush\n")
	jobs, wait = parallel(para)
	for i := 0; i < keys; i++ {
		key := strconv.Itoa(i)
		jobs <- func() {
			resp := client.DoCache(ctx, client.B().Get().Key(key).Cache(), time.Minute)
			if !IsRedisNil(resp.Error()) {
				t.Fatalf("unexpected csc get response after flush %v", resp)
			}
			if resp.IsCacheHit() {
				t.Fatalf("unexpected csc cache hit after flush")
			}
		}
	}
	wait()
}

func testSETGETCSC(t *testing.T, client Client) {
	testSETGET(t, client, true)
}

func testSETGETRESP2(t *testing.T, client Client) {
	testSETGET(t, client, false)
}

//gocyclo:ignore
func testSETGET(t *testing.T, client Client, csc bool) {
	ctx := context.Background()
	keys := 10000
	para := 8

	kvs := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		kvs[strconv.Itoa(i)] = strconv.FormatInt(rand.Int63(), 10)
	}

	t.Logf("testing SET with %d keys and %d parallelism\n", keys, para)
	jobs, wait := parallel(para)
	for i := 0; i < keys; i++ {
		key := strconv.Itoa(i)
		jobs <- func() {
			val, err := client.Do(ctx, client.B().Set().Key(key).Value(kvs[key]).Build()).ToString()
			if err != nil || val != "OK" {
				t.Fatalf("unexpected set response %v %v", val, err)
			}
		}
	}
	wait()

	t.Logf("testing GET with %d keys and %d parallelism\n", keys*2, para)
	jobs, wait = parallel(para)
	for i := 0; i < keys*2; i++ {
		key := strconv.Itoa(rand.Intn(keys * 2))
		jobs <- func() {
			val, err := client.Do(ctx, client.B().Get().Key(key).Build()).ToString()
			if v, ok := kvs[key]; !((ok && val == v) || (!ok && IsRedisNil(err))) {
				t.Fatalf("unexpected get response %v %v %v", val, err, ok)
			}
		}
	}
	wait()

	t.Logf("testing client side caching with %d interations and %d parallelism\n", keys*5, para)
	jobs, wait = parallel(para)
	hits, miss := int64(0), int64(0)
	for i := 0; i < keys*10; i++ {
		key := strconv.Itoa(rand.Intn(keys / 100))
		jobs <- func() {
			resp := client.DoCache(ctx, client.B().Get().Key(key).Cache(), time.Minute)
			val, err := resp.ToString()
			if v, ok := kvs[key]; !((ok && val == v) || (!ok && IsRedisNil(err))) {
				t.Fatalf("unexpected csc get response %v %v %v", val, err, ok)
			}
			if resp.IsCacheHit() {
				atomic.AddInt64(&hits, 1)
			} else {
				atomic.AddInt64(&miss, 1)
			}
		}
	}
	wait()
	if csc {
		if atomic.LoadInt64(&miss) != 100 || atomic.LoadInt64(&hits) != int64(keys*10-100) {
			t.Fatalf("unexpected client side caching hits and miss %v %v", atomic.LoadInt64(&hits), atomic.LoadInt64(&miss))
		}
	} else {
		if atomic.LoadInt64(&hits) != 0 {
			t.Fatalf("unexpected client side caching hits and miss %v %v", atomic.LoadInt64(&hits), atomic.LoadInt64(&miss))
		}
	}

	t.Logf("testing DEL with %d keys and %d parallelism\n", keys*2, para)
	jobs, wait = parallel(para)
	for i := 0; i < keys*2; i++ {
		key := strconv.Itoa(i)
		jobs <- func() {
			val, err := client.Do(ctx, client.B().Del().Key(key).Build()).AsInt64()
			if _, ok := kvs[key]; !((val == 1 && ok) || (val == 0 && !ok)) {
				t.Fatalf("unexpected del response %v %v %v", val, err, ok)
			}
		}
	}
	wait()

	t.Logf("testing client side caching after delete\n")
	jobs, wait = parallel(para)
	for i := 0; i < keys/100; i++ {
		key := strconv.Itoa(i)
		jobs <- func() {
			resp := client.DoCache(ctx, client.B().Get().Key(key).Cache(), time.Minute)
			if !IsRedisNil(resp.Error()) {
				t.Fatalf("unexpected csc get response after delete %v", resp)
			}
			if resp.IsCacheHit() {
				t.Fatalf("unexpected csc cache hit after delete")
			}
		}
	}
	wait()
}

func testMultiSETGETCSC(t *testing.T, client Client) {
	testMultiSETGET(t, client, true)
}

func testMultiSETGETRESP2(t *testing.T, client Client) {
	testMultiSETGET(t, client, false)
}

func testMultiSETGETCSCHelpers(t *testing.T, client Client) {
	testMultiSETGETHelpers(t, client, true)
}

func testMultiSETGETRESP2Helpers(t *testing.T, client Client) {
	testMultiSETGETHelpers(t, client, false)
}

//gocyclo:ignore
func testMultiSETGET(t *testing.T, client Client, csc bool) {
	ctx := context.Background()
	keys := 10000
	batch := 100
	para := 8

	kvs := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		kvs["m"+strconv.Itoa(i)] = strconv.FormatInt(rand.Int63(), 10)
	}

	t.Logf("testing Multi SET with %d keys and %d parallelism\n", keys, para)
	jobs, wait := parallel(para)
	for i := 0; i < keys; i += batch {
		commands := make(Commands, 0, batch)
		for j := 0; j < batch; j++ {
			key := "m" + strconv.Itoa(i+j)
			commands = append(commands, client.B().Set().Key(key).Value(kvs[key]).Build())
		}
		jobs <- func() {
			for _, resp := range client.DoMulti(ctx, commands...) {
				val, err := resp.ToString()
				if err != nil || val != "OK" {
					t.Fatalf("unexpected set response %v %v", val, err)
				}
			}
		}
	}
	wait()

	t.Logf("testing GET with %d keys and %d parallelism\n", keys*2, para)
	jobs, wait = parallel(para)
	for i := 0; i < keys*2; i += batch {
		cmdkeys := make([]string, 0, batch)
		commands := make(Commands, 0, batch)
		for j := 0; j < batch; j++ {
			cmdkeys = append(cmdkeys, "m"+strconv.Itoa(rand.Intn(keys*2)))
			commands = append(commands, client.B().Get().Key(cmdkeys[len(cmdkeys)-1]).Build())
		}
		jobs <- func() {
			for j, resp := range client.DoMulti(ctx, commands...) {
				val, err := resp.ToString()
				if v, ok := kvs[cmdkeys[j]]; !((ok && val == v) || (!ok && IsRedisNil(err))) {
					t.Fatalf("unexpected get response %v %v %v", val, err, ok)
				}
			}
		}
	}
	wait()

	t.Logf("testing client side caching with %d interations and %d parallelism\n", keys*5, para)
	jobs, wait = parallel(para)
	hits, miss := int64(0), int64(0)
	for i := 0; i < keys*10; i += batch {
		cmdkeys := make([]string, 0, batch)
		commands := make([]CacheableTTL, 0, batch)
		for j := 0; j < batch; j++ {
			cmdkeys = append(cmdkeys, "m"+strconv.Itoa(rand.Intn(keys/100)))
			commands = append(commands, CT(client.B().Get().Key(cmdkeys[len(cmdkeys)-1]).Cache(), time.Minute))
		}
		jobs <- func() {
			for j, resp := range client.DoMultiCache(ctx, commands...) {
				val, err := resp.ToString()
				if v, ok := kvs[cmdkeys[j]]; !((ok && val == v) || (!ok && IsRedisNil(err))) {
					t.Fatalf("unexpected csc get response %v %v %v", val, err, ok)
				}
				if resp.IsCacheHit() {
					atomic.AddInt64(&hits, 1)
				} else {
					atomic.AddInt64(&miss, 1)
				}
			}
		}
	}
	wait()
	if csc {
		if atomic.LoadInt64(&miss) != 100 || atomic.LoadInt64(&hits) != int64(keys*10-100) {
			t.Fatalf("unexpected client side caching hits and miss %v %v", atomic.LoadInt64(&hits), atomic.LoadInt64(&miss))
		}
	} else {
		if atomic.LoadInt64(&hits) != 0 {
			t.Fatalf("unexpected client side caching hits and miss %v %v", atomic.LoadInt64(&hits), atomic.LoadInt64(&miss))
		}
	}

	t.Logf("testing DEL with %d keys and %d parallelism\n", keys*2, para)
	jobs, wait = parallel(para)
	for i := 0; i < keys*2; i += batch {
		cmdkeys := make([]string, 0, batch)
		commands := make(Commands, 0, batch)
		for j := 0; j < batch; j++ {
			cmdkeys = append(cmdkeys, "m"+strconv.Itoa(i+j))
			commands = append(commands, client.B().Del().Key(cmdkeys[len(cmdkeys)-1]).Build())
		}
		jobs <- func() {
			for j, resp := range client.DoMulti(ctx, commands...) {
				val, err := resp.AsInt64()
				if _, ok := kvs[cmdkeys[j]]; !((val == 1 && ok) || (val == 0 && !ok)) {
					t.Fatalf("unexpected del response %v %v %v", val, err, ok)
				}
			}
		}
	}
	wait()

	t.Logf("testing client side caching after delete\n")
	jobs, wait = parallel(para)
	for i := 0; i < keys/100; i += batch {
		cmdkeys := make([]string, 0, batch)
		commands := make([]CacheableTTL, 0, batch)
		for j := 0; j < batch; j++ {
			cmdkeys = append(cmdkeys, "m"+strconv.Itoa(i+j))
			commands = append(commands, CT(client.B().Get().Key(cmdkeys[len(cmdkeys)-1]).Cache(), time.Minute))
		}
		jobs <- func() {
			for _, resp := range client.DoMultiCache(ctx, commands...) {
				if !IsRedisNil(resp.Error()) {
					t.Fatalf("unexpected csc get response after delete %v", resp)
				}
				if resp.IsCacheHit() {
					t.Fatalf("unexpected csc cache hit after delete")
				}
			}
		}
	}
	wait()
}

func testMultiSETGETHelpers(t *testing.T, client Client, csc bool) {
	ctx := context.Background()
	keys := 10000

	kvs := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		kvs["z"+strconv.Itoa(i)] = strconv.FormatInt(rand.Int63(), 10)
	}

	t.Logf("testing Multi SET with %d keys\n", keys)
	for _, err := range MSet(client, ctx, kvs) {
		if err != nil {
			t.Fatalf("unexpecetd err %v\n", err)
		}
	}

	t.Logf("testing GET with %d keys\n", keys*2)
	cmdKeys := make([]string, keys*2)
	for i := 0; i < len(cmdKeys); i++ {
		cmdKeys[i] = "z" + strconv.Itoa(i)
	}
	validate := func(resp map[string]RedisMessage) {
		for _, key := range cmdKeys {
			ret, ok := resp[key]
			if !ok {
				t.Fatalf("unexpecetd result %v not found\n", key)
			}
			if exp, ok := kvs[key]; ok {
				if exp != ret.string {
					t.Fatalf("unexpecetd result %v wrong value %v\n", key, exp)
				}
			} else {
				if !ret.IsNil() {
					t.Fatalf("unexpecetd result %v wrong value %v\n", key, "nil")
				}
			}
		}
	}
	resp, err := MGet(client, ctx, cmdKeys)
	if err != nil {
		t.Fatalf("unexpecetd err %v\n", err)
	}
	validate(resp)

	t.Logf("testing client side caching with %d keys\n", keys*2)
	resp, err = MGetCache(client, ctx, time.Minute, cmdKeys)
	if err != nil {
		t.Fatalf("unexpecetd err %v\n", err)
	}
	validate(resp)
	for _, ret := range resp {
		if ret.IsCacheHit() {
			t.Fatalf("unexpeceted cache hit %v\n", ret)
		}
	}
	resp, err = MGetCache(client, ctx, time.Minute, cmdKeys)
	if err != nil {
		t.Fatalf("unexpecetd err %v\n", err)
	}
	validate(resp)
	for _, ret := range resp {
		if csc && !ret.IsCacheHit() {
			t.Fatalf("unexpeceted cache miss %v\n", ret)
		}
	}

	t.Logf("testing DEL with %d keys\n", keys*2)
	for _, err := range MDel(client, ctx, cmdKeys) {
		if err != nil {
			t.Fatalf("unexpecetd err %v\n", err)
		}
	}
	time.Sleep(time.Second)
	t.Logf("testing client side caching after delete\n")
	resp, err = MGetCache(client, ctx, time.Minute, cmdKeys)
	if err != nil {
		t.Fatalf("unexpecetd err %v\n", err)
	}
	for _, ret := range resp {
		if !ret.IsNil() {
			t.Fatalf("unexpeceted cache hit %v\n", ret)
		}
	}
}

//gocyclo:ignore
func testMultiExec(t *testing.T, client Client) {
	ctx := context.Background()
	keys := 1000
	para := 8

	kvs := make(map[string]int64, keys)
	for i := 1; i <= keys; i++ {
		kvs["me"+strconv.Itoa(i)] = int64(i)
	}

	t.Logf("testing MULTI EXEC with %d keys and %d parallelism\n", keys, para)
	jobs, wait := parallel(para)
	for k, v := range kvs {
		k, v := k, v
		jobs <- func() {
			resps, err := client.DoMulti(ctx,
				client.B().Multi().Build(),
				client.B().Set().Key(k).Value(strconv.FormatInt(v, 10)).ExSeconds(v).Build(),
				client.B().Ttl().Key(k).Build(),
				client.B().Get().Key(k).Build(),
				client.B().Exec().Build(),
			)[4].ToArray()
			if err != nil {
				t.Fatalf("unexpected exec response %v", err)
			}
			if resps[1].integer != v {
				t.Fatalf("unexpected ttl response %v %v", v, resps[1].integer)
			}
			if resps[2].string != strconv.FormatInt(v, 10) {
				t.Fatalf("unexpected get response %v %v", v, resps[2].string)
			}
		}
	}
	wait()
}

func testBlockingZPOP(t *testing.T, client Client) {
	ctx := context.Background()
	key := "bz_pop_test"
	items := 2000

	client.Do(ctx, client.B().Del().Key(key).Build())

	t.Logf("testing BZPOPMIN blocking concurrently with ZADD with %d items\n", items)
	go func() {
		for i := 0; i < items; i++ {
			v, err := client.Do(ctx, client.B().Zadd().Key(key).ScoreMember().ScoreMember(float64(i), strconv.Itoa(i)).Build()).AsInt64()
			if err != nil || v != 1 {
				t.Errorf("unexpected ZADD response %v %v", v, err)
			}
		}
	}()
	for i := 0; i < items; i++ {
		arr, err := client.Do(ctx, client.B().Bzpopmin().Key(key).Timeout(0).Build()).AsStrSlice()
		if err != nil || (arr[0] != key || arr[1] != arr[2] || arr[1] != strconv.Itoa(i)) {
			t.Fatalf("unexpected BZPOPMIN response %v %v", arr, err)
		}
	}
	client.Do(ctx, client.B().Del().Key(key).Build())
}

func testBlockingXREAD(t *testing.T, client Client) {
	ctx := context.Background()
	key := "xread_test"
	items := 2000

	client.Do(ctx, client.B().Del().Key(key).Build())

	t.Logf("testing blocking XREAD concurrently with XADD with %d items\n", items)
	go func() {
		for i := 0; i < items; i++ {
			v := strconv.Itoa(i)
			v, err := client.Do(ctx, client.B().Xadd().Key(key).Id("*").FieldValue().FieldValue(v, v).Build()).ToString()
			if err != nil || v == "" {
				t.Errorf("unexpected XADD response %v %v", v, err)
			}
		}
	}()
	id := "0"
	for i := 0; i < items; i++ {
		m, err := client.Do(ctx, client.B().Xread().Count(1).Block(0).Streams().Key(key).Id(id).Build()).AsXRead()
		if err != nil {
			t.Fatalf("unexpected XREAD response %v %v", m, err)
		}
		id = m[key][0].ID
		if len(m[key][0].FieldValues) == 0 {
			t.Fatalf("unexpected XREAD response %v %v", m, err)
		}
		for f, v := range m[key][0].FieldValues {
			if f != v || f != strconv.Itoa(i) {
				t.Fatalf("unexpected XREAD response %v %v", m, err)
			}
		}
	}
	client.Do(ctx, client.B().Del().Key(key).Build())
}

func testPubSub(t *testing.T, client Client) {
	msgs := 5000
	mmap := make(map[string]struct{})
	for i := 0; i < msgs; i++ {
		mmap[strconv.Itoa(i)] = struct{}{}
	}
	t.Logf("testing pubsub with %v messages\n", msgs)
	jobs, wait := parallel(10)

	ctx := context.Background()

	messages := make(chan string, 10)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		err := client.Receive(ctx, client.B().Subscribe().Channel("ch1").Build(), func(msg PubSubMessage) {
			messages <- msg.Message
		})
		if err != nil {
			t.Errorf("unexpected subscribe response %v", err)
		}
		wg.Done()
	}()

	go func() {
		err := client.Receive(ctx, client.B().Psubscribe().Pattern("pat*").Build(), func(msg PubSubMessage) {
			messages <- msg.Message
		})
		if err != nil {
			t.Errorf("unexpected subscribe response %v", err)
		}
		wg.Done()
	}()

	go func() {
		time.Sleep(time.Second)
		for i := 0; i < msgs; i++ {
			msg := strconv.Itoa(i)
			ch := "ch1"
			if i%10 == 0 {
				ch = "pat1"
			}
			jobs <- func() {
				if err := client.Do(context.Background(), client.B().Publish().Channel(ch).Message(msg).Build()).Error(); err != nil {
					t.Errorf("unexpected publish response %v", err)
				}
			}
		}
		wait()
	}()

	for message := range messages {
		delete(mmap, message)
		if len(mmap) == 0 {
			close(messages)
		}
	}

	for _, c := range client.Nodes() {
		for _, resp := range c.DoMulti(context.Background(),
			client.B().Unsubscribe().Channel("ch1").Build(),
			client.B().Punsubscribe().Pattern("pat*").Build()) {
			if err := resp.Error(); err != nil {
				t.Fatal(err)
			}
		}
	}
	wg.Wait()

	t.Logf("testing pubsub hooks with 500 messages\n")

	for i := 0; i < 500; i++ {
		cc, cancel := client.Dedicate()
		msg := strconv.Itoa(i)
		ch := cc.SetPubSubHooks(PubSubHooks{
			OnMessage: func(m PubSubMessage) {
				cc.SetPubSubHooks(PubSubHooks{})
			},
		})
		if err := cc.Do(context.Background(), client.B().Subscribe().Channel("ch2").Build()).Error(); err != nil {
			t.Fatal(err)
		}
		if err := client.Do(context.Background(), client.B().Publish().Channel("ch2").Message(msg).Build()).Error(); err != nil {
			t.Fatal(err)
		}
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
		cancel()
	}
}

func testLua(t *testing.T, client Client) {
	script := NewLuaScript("return {KEYS[1],ARGV[1]}")

	keys := 1000
	para := 4
	kvs := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		kvs["m"+strconv.Itoa(i)] = strconv.FormatInt(rand.Int63(), 10)
	}

	t.Logf("testing lua with %d keys and %d parallelism\n", keys, para)
	jobs, wait := parallel(para)
	for k, v := range kvs {
		k := k
		v := v
		jobs <- func() {
			val, err := script.Exec(context.Background(), client, []string{k}, []string{v}).AsStrSlice()
			if err != nil || !reflect.DeepEqual(val, []string{k, v}) {
				t.Fatalf("unexpected lua response %v %v", val, err)
			}
		}
	}
	wait()
}

func run(t *testing.T, client Client, cases ...func(*testing.T, Client)) {
	wg := sync.WaitGroup{}
	wg.Add(len(cases))
	for _, c := range cases {
		c := c
		go func() {
			c(t, client)
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestSingleClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer ShouldNotLeaked(SetupLeakDetection())
	client, err := NewClient(ClientOption{
		InitAddress:      []string{"127.0.0.1:6379"},
		ConnWriteTimeout: 180 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	run(t, client, testSETGETCSC, testMultiSETGETCSC, testMultiSETGETCSCHelpers, testMultiExec, testBlockingZPOP, testBlockingXREAD, testPubSub, testLua)
	run(t, client, testFlush)

	client.Close()
}

func TestSentinelClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer ShouldNotLeaked(SetupLeakDetection())
	client, err := NewClient(ClientOption{
		InitAddress:      []string{"127.0.0.1:26379"},
		ConnWriteTimeout: 180 * time.Second,
		Sentinel: SentinelOption{
			MasterSet: "test",
		},
		SelectDB: 2, // https://github.com/redis/rueidis/issues/138
	})
	if err != nil {
		t.Fatal(err)
	}

	run(t, client, testSETGETCSC, testMultiSETGETCSC, testMultiSETGETCSCHelpers, testMultiExec, testBlockingZPOP, testBlockingXREAD, testPubSub, testLua)
	run(t, client, testFlush)

	client.Close()
}

func TestClusterClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer ShouldNotLeaked(SetupLeakDetection())
	client, err := NewClient(ClientOption{
		InitAddress:      []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"},
		ConnWriteTimeout: 180 * time.Second,
		ShuffleInit:      true,
		Dialer:           net.Dialer{KeepAlive: -1},
	})
	if err != nil {
		t.Fatal(err)
	}
	run(t, client, testSETGETCSC, testMultiSETGETCSC, testMultiSETGETCSCHelpers, testMultiExec, testBlockingZPOP, testBlockingXREAD, testPubSub, testLua)

	client.Close()
}

func TestSingleClient5Integration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer ShouldNotLeaked(SetupLeakDetection())
	client, err := NewClient(ClientOption{
		InitAddress:      []string{"127.0.0.1:6355"},
		ConnWriteTimeout: 180 * time.Second,
		DisableCache:     true,
	})
	if err != nil {
		t.Fatal(err)
	}

	run(t, client, testSETGETRESP2, testMultiSETGETRESP2, testMultiSETGETRESP2Helpers, testMultiExec, testBlockingZPOP, testBlockingXREAD, testPubSub, testLua)

	client.Close()
}

func TestCluster5ClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer ShouldNotLeaked(SetupLeakDetection())
	client, err := NewClient(ClientOption{
		InitAddress:      []string{"127.0.0.1:7004", "127.0.0.1:7005", "127.0.0.1:7006"},
		ConnWriteTimeout: 180 * time.Second,
		ShuffleInit:      true,
		DisableCache:     true,
		Dialer:           net.Dialer{KeepAlive: -1},
	})
	if err != nil {
		t.Fatal(err)
	}
	run(t, client, testSETGETRESP2, testMultiSETGETRESP2, testMultiSETGETRESP2Helpers, testMultiExec, testBlockingZPOP, testBlockingXREAD, testPubSub, testLua)

	client.Close()
}

func TestSentinel5ClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer ShouldNotLeaked(SetupLeakDetection())
	client, err := NewClient(ClientOption{
		InitAddress:      []string{"127.0.0.1:26355"},
		ConnWriteTimeout: 180 * time.Second,
		DisableCache:     true,
		Sentinel: SentinelOption{
			MasterSet: "test5",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	run(t, client, testSETGETRESP2, testMultiSETGETRESP2, testMultiSETGETRESP2Helpers, testMultiExec, testBlockingZPOP, testBlockingXREAD, testPubSub, testLua)

	client.Close()
}

func TestKeyDBSingleClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer ShouldNotLeaked(SetupLeakDetection())
	client, err := NewClient(ClientOption{
		InitAddress:      []string{"127.0.0.1:6344"},
		ConnWriteTimeout: 180 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	run(t, client, testSETGETCSC, testMultiSETGETCSC, testMultiSETGETCSCHelpers, testMultiExec, testBlockingZPOP, testBlockingXREAD, testPubSub, testLua)
	run(t, client, testFlush)

	client.Close()
}

func TestDragonflyDBSingleClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer ShouldNotLeaked(SetupLeakDetection())
	client, err := NewClient(ClientOption{
		InitAddress:      []string{"127.0.0.1:6333"},
		ConnWriteTimeout: 180 * time.Second,
		DisableCache:     true,
	})
	if err != nil {
		t.Fatal(err)
	}

	run(t, client, testSETGETRESP2, testMultiSETGETRESP2, testMultiSETGETRESP2Helpers, testPubSub, testLua)

	client.Close()
}

func TestKvrocksSingleClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	defer ShouldNotLeaked(SetupLeakDetection())
	client, err := NewClient(ClientOption{
		InitAddress:      []string{"127.0.0.1:6666"},
		ConnWriteTimeout: 180 * time.Second,
		DisableCache:     true,
	})
	if err != nil {
		t.Fatal(err)
	}

	run(t, client, testSETGETRESP2, testMultiSETGETRESP2, testMultiSETGETRESP2Helpers, testPubSub, testLua)
	run(t, client, testFlush)

	client.Close()
}
