package rueidis

import (
	"context"
	"reflect"
	"strconv"
	"testing"
)

//gocyclo:ignore
func TestMGetCache(t *testing.T) {
	defer ShouldNotLeaked(SetupLeakDetection())
	t.Run("single client", func(t *testing.T) {
		m := &mockConn{}
		client, err := newSingleClient(&ClientOption{InitAddress: []string{""}}, m, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate DoMultiCache", func(t *testing.T) {
			m.DoMultiCacheFn = func(multi ...CacheableTTL) []RedisResult {
				result := make([]RedisResult, 0, 2)
				for _, cmd := range multi {
					if reflect.DeepEqual(cmd.Cmd.Commands(), []string{"GET", "1"}) && cmd.TTL == 100 {
						result = append(result, newResult(RedisMessage{typ: '+', string: "1"}, nil))
					}
					if reflect.DeepEqual(cmd.Cmd.Commands(), []string{"GET", "2"}) && cmd.TTL == 100 {
						result = append(result, newResult(RedisMessage{typ: '+', string: "2"}, nil))
					}
				}
				if len(result) != 2 {
					t.Fatalf("invalid multi commands: %+v", multi)
				}

				return result
			}
			if v, err := MGetCache(client, context.Background(), 100, []string{"1", "2"}); err != nil || v["1"].string != "1" || v["2"].string != "2" {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate DoMultiCache Empty", func(t *testing.T) {
			if v, err := MGetCache(client, context.Background(), 100, []string{}); err != nil || v == nil {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate DoMultiCache Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoMultiCacheFn = func(multi ...CacheableTTL) []RedisResult {
				return []RedisResult{newResult(RedisMessage{}, context.Canceled), newResult(RedisMessage{}, context.Canceled)}
			}

			if v, err := MGetCache(client, ctx, 100, []string{"1", "2"}); err != context.Canceled {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
	})
	t.Run("cluster client", func(t *testing.T) {
		m := &mockConn{
			DoFn: func(cmd Completed) RedisResult {
				return slotsResp
			},
		}
		client, err := newClusterClient(&ClientOption{InitAddress: []string{":0"}}, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate DoMultiCache", func(t *testing.T) {
			keys := make([]string, 100)
			for i := range keys {
				keys[i] = strconv.Itoa(i)
			}
			m.DoMultiCacheFn = func(multi ...CacheableTTL) []RedisResult {
				result := make([]RedisResult, 0, len(multi))
				for _, key := range keys {
					for _, cmd := range multi {
						if reflect.DeepEqual(cmd.Cmd.Commands(), []string{"GET", key}) && cmd.TTL == 100 {
							result = append(result, newResult(RedisMessage{typ: '+', string: key}, nil))
						}
					}
				}
				if len(result) != len(multi) {
					t.Fatalf("unexpected multi command: %+v", multi)
				}

				return result
			}
			v, err := MGetCache(client, context.Background(), 100, keys)
			if err != nil {
				t.Fatalf("unexpected response %v %v", v, err)
			}
			for _, key := range keys {
				if v[key].string != key {
					t.Fatalf("unexpected response %v", v)
				}
			}
		})
		t.Run("Delegate DoCache Empty", func(t *testing.T) {
			if v, err := MGetCache(client, context.Background(), 100, []string{}); err != nil || v == nil {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate DoCache Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoMultiCacheFn = func(multi ...CacheableTTL) []RedisResult {
				return []RedisResult{newResult(RedisMessage{}, context.Canceled), newResult(RedisMessage{}, context.Canceled)}
			}

			if v, err := MGetCache(client, ctx, 100, []string{"1", "2"}); err != context.Canceled {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
	})
}

//gocyclo:ignore
func TestMGet(t *testing.T) {
	defer ShouldNotLeaked(SetupLeakDetection())
	t.Run("single client", func(t *testing.T) {
		m := &mockConn{}
		client, err := newSingleClient(&ClientOption{InitAddress: []string{""}}, m, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate Do", func(t *testing.T) {
			m.DoFn = func(cmd Completed) RedisResult {
				if !reflect.DeepEqual(cmd.Commands(), []string{"MGET", "1", "2"}) {
					t.Fatalf("unexpected command %v", cmd)
				}
				return newResult(RedisMessage{typ: '*', values: []RedisMessage{{typ: '+', string: "1"}, {typ: '+', string: "2"}}}, nil)
			}
			if v, err := MGet(client, context.Background(), []string{"1", "2"}); err != nil || v["1"].string != "1" || v["2"].string != "2" {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate Do Empty", func(t *testing.T) {
			if v, err := MGet(client, context.Background(), []string{}); err != nil || v == nil {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate Do Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoFn = func(cmd Completed) RedisResult {
				return newResult(RedisMessage{}, context.Canceled)
			}
			if v, err := MGet(client, ctx, []string{"1", "2"}); err != context.Canceled {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
	})
	t.Run("cluster client", func(t *testing.T) {
		m := &mockConn{
			DoFn: func(cmd Completed) RedisResult {
				return slotsResp
			},
		}
		client, err := newClusterClient(&ClientOption{InitAddress: []string{":0"}}, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate DoMulti", func(t *testing.T) {
			keys := make([]string, 100)
			for i := range keys {
				keys[i] = strconv.Itoa(i)
			}
			m.DoMultiFn = func(multi ...Completed) []RedisResult {
				result := make([]RedisResult, 0, len(multi))
				for _, key := range keys {
					for _, cmd := range multi {
						if reflect.DeepEqual(cmd.Commands(), []string{"GET", key}) {
							result = append(result, newResult(RedisMessage{typ: '+', string: key}, nil))
						}
					}
				}
				if len(result) != len(multi) {
					t.Fatalf("unexpected multi command: %+v", multi)
				}

				return result
			}
			v, err := MGet(client, context.Background(), keys)
			if err != nil {
				t.Fatalf("unexpected response %v %v", v, err)
			}
			for _, key := range keys {
				if v[key].string != key {
					t.Fatalf("unexpected response %v", v)
				}
			}
		})
		t.Run("Delegate Do Empty", func(t *testing.T) {
			if v, err := MGet(client, context.Background(), []string{}); err != nil || v == nil {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate DoMulti Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoMultiFn = func(multi ...Completed) []RedisResult {
				return []RedisResult{newResult(RedisMessage{}, context.Canceled), newResult(RedisMessage{}, context.Canceled)}
			}
			if v, err := MGet(client, ctx, []string{"1", "2"}); err != context.Canceled {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
	})
}

//gocyclo:ignore
func TestMDel(t *testing.T) {
	defer ShouldNotLeaked(SetupLeakDetection())
	t.Run("single client", func(t *testing.T) {
		m := &mockConn{}
		client, err := newSingleClient(&ClientOption{InitAddress: []string{""}}, m, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate Do", func(t *testing.T) {
			m.DoFn = func(cmd Completed) RedisResult {
				if !reflect.DeepEqual(cmd.Commands(), []string{"DEL", "1", "2"}) {
					t.Fatalf("unexpected command %v", cmd)
				}
				return newResult(RedisMessage{typ: ':', integer: 2}, nil)
			}
			if v := MDel(client, context.Background(), []string{"1", "2"}); v["1"] != nil || v["2"] != nil {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate Do Empty", func(t *testing.T) {
			if v := MDel(client, context.Background(), []string{}); len(v) != 0 {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate Do Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoFn = func(cmd Completed) RedisResult {
				return newResult(RedisMessage{}, context.Canceled)
			}
			if v := MDel(client, ctx, []string{"1", "2"}); v["1"] != context.Canceled || v["2"] != context.Canceled {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
	})
	t.Run("cluster client", func(t *testing.T) {
		m := &mockConn{
			DoFn: func(cmd Completed) RedisResult {
				return slotsResp
			},
		}
		client, err := newClusterClient(&ClientOption{InitAddress: []string{":0"}}, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate Do", func(t *testing.T) {
			keys := make([]string, 100)
			for i := range keys {
				keys[i] = strconv.Itoa(i)
			}
			m.DoFn = func(cmd Completed) RedisResult {
				for _, key := range keys {
					if reflect.DeepEqual(cmd.Commands(), []string{"DEL", key}) {
						return newResult(RedisMessage{typ: ':', integer: 1}, nil)
					}
				}
				t.Fatalf("unexpected command %v", cmd)
				return RedisResult{}
			}
			v := MDel(client, context.Background(), keys)
			for _, key := range keys {
				if v[key] != nil {
					t.Fatalf("unexpected response %v", v)
				}
			}
		})
		t.Run("Delegate Do Empty", func(t *testing.T) {
			if v := MDel(client, context.Background(), []string{}); len(v) != 0 {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate Do Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoFn = func(cmd Completed) RedisResult {
				return newResult(RedisMessage{}, context.Canceled)
			}
			if v := MDel(client, ctx, []string{"1", "2"}); v["1"] != context.Canceled || v["2"] != context.Canceled {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
	})
}

func TestMSet(t *testing.T) {
	defer ShouldNotLeaked(SetupLeakDetection())
	t.Run("single client", func(t *testing.T) {
		m := &mockConn{}
		client, err := newSingleClient(&ClientOption{InitAddress: []string{""}}, m, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate Do", func(t *testing.T) {
			m.DoFn = func(cmd Completed) RedisResult {
				if !reflect.DeepEqual(cmd.Commands(), []string{"MSET", "1", "1", "2", "2"}) &&
					!reflect.DeepEqual(cmd.Commands(), []string{"MSET", "2", "2", "1", "1"}) {
					t.Fatalf("unexpected command %v", cmd)
				}
				return newResult(RedisMessage{typ: '+', string: "OK"}, nil)
			}
			if err := MSet(client, context.Background(), map[string]string{"1": "1", "2": "2"}); err["1"] != nil || err["2"] != nil {
				t.Fatalf("unexpected response %v", err)
			}
		})
		t.Run("Delegate Do Empty", func(t *testing.T) {
			if err := MSet(client, context.Background(), map[string]string{}); len(err) != 0 {
				t.Fatalf("unexpected response %v", err)
			}
		})
		t.Run("Delegate Do Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoFn = func(cmd Completed) RedisResult {
				return newResult(RedisMessage{}, context.Canceled)
			}
			if err := MSet(client, ctx, map[string]string{"1": "1", "2": "2"}); err["1"] != context.Canceled || err["2"] != context.Canceled {
				t.Fatalf("unexpected response %v", err)
			}
		})
	})
	t.Run("cluster client", func(t *testing.T) {
		m := &mockConn{
			DoFn: func(cmd Completed) RedisResult {
				return slotsResp
			},
		}
		client, err := newClusterClient(&ClientOption{InitAddress: []string{":0"}}, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate Do", func(t *testing.T) {
			keys := make(map[string]string, 100)
			for i := 0; i < 100; i++ {
				keys[strconv.Itoa(i)] = strconv.Itoa(i)
			}
			m.DoFn = func(cmd Completed) RedisResult {
				for key := range keys {
					if reflect.DeepEqual(cmd.Commands(), []string{"MSET", key, key}) {
						return newResult(RedisMessage{typ: '+', string: "OK"}, nil)
					}
				}
				t.Fatalf("unexpected command %v", cmd)
				return RedisResult{}
			}
			err := MSet(client, context.Background(), keys)
			for key := range keys {
				if err[key] != nil {
					t.Fatalf("unexpected response %v", err)
				}
			}
		})
		t.Run("Delegate Do Empty", func(t *testing.T) {
			if err := MSet(client, context.Background(), map[string]string{}); len(err) != 0 {
				t.Fatalf("unexpected response %v", err)
			}
		})
		t.Run("Delegate Do Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoFn = func(cmd Completed) RedisResult {
				return newResult(RedisMessage{}, context.Canceled)
			}
			if err := MSet(client, ctx, map[string]string{"1": "1", "2": "2"}); err["1"] != context.Canceled || err["2"] != context.Canceled {
				t.Fatalf("unexpected response %v", err)
			}
		})
	})
}

func TestMSetNX(t *testing.T) {
	defer ShouldNotLeaked(SetupLeakDetection())
	t.Run("single client", func(t *testing.T) {
		m := &mockConn{}
		client, err := newSingleClient(&ClientOption{InitAddress: []string{""}}, m, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate Do", func(t *testing.T) {
			m.DoFn = func(cmd Completed) RedisResult {
				if !reflect.DeepEqual(cmd.Commands(), []string{"MSETNX", "1", "1", "2", "2"}) &&
					!reflect.DeepEqual(cmd.Commands(), []string{"MSETNX", "2", "2", "1", "1"}) {
					t.Fatalf("unexpected command %v", cmd)
				}
				return newResult(RedisMessage{typ: '+', string: "OK"}, nil)
			}
			if err := MSetNX(client, context.Background(), map[string]string{"1": "1", "2": "2"}); err["1"] != nil || err["2"] != nil {
				t.Fatalf("unexpected response %v", err)
			}
		})
		t.Run("Delegate Do Empty", func(t *testing.T) {
			if err := MSetNX(client, context.Background(), map[string]string{}); len(err) != 0 {
				t.Fatalf("unexpected response %v", err)
			}
		})
		t.Run("Delegate Do Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoFn = func(cmd Completed) RedisResult {
				return newResult(RedisMessage{}, context.Canceled)
			}
			if err := MSetNX(client, ctx, map[string]string{"1": "1", "2": "2"}); err["1"] != context.Canceled || err["2"] != context.Canceled {
				t.Fatalf("unexpected response %v", err)
			}
		})
	})
	t.Run("cluster client", func(t *testing.T) {
		m := &mockConn{
			DoFn: func(cmd Completed) RedisResult {
				return slotsResp
			},
		}
		client, err := newClusterClient(&ClientOption{InitAddress: []string{":0"}}, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate Do", func(t *testing.T) {
			keys := make(map[string]string, 100)
			for i := 0; i < 100; i++ {
				keys[strconv.Itoa(i)] = strconv.Itoa(i)
			}
			m.DoFn = func(cmd Completed) RedisResult {
				for key := range keys {
					if reflect.DeepEqual(cmd.Commands(), []string{"MSETNX", key, key}) {
						return newResult(RedisMessage{typ: '+', string: "OK"}, nil)
					}
				}
				t.Fatalf("unexpected command %v", cmd)
				return RedisResult{}
			}
			err := MSetNX(client, context.Background(), keys)
			for key := range keys {
				if err[key] != nil {
					t.Fatalf("unexpected response %v", err)
				}
			}
		})
		t.Run("Delegate Do Empty", func(t *testing.T) {
			if err := MSetNX(client, context.Background(), map[string]string{}); len(err) != 0 {
				t.Fatalf("unexpected response %v", err)
			}
		})
		t.Run("Delegate Do Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoFn = func(cmd Completed) RedisResult {
				return newResult(RedisMessage{}, context.Canceled)
			}
			if err := MSetNX(client, ctx, map[string]string{"1": "1", "2": "2"}); err["1"] != context.Canceled || err["2"] != context.Canceled {
				t.Fatalf("unexpected response %v", err)
			}
		})
	})
}

func TestMSetNXNotSet(t *testing.T) {
	defer ShouldNotLeaked(SetupLeakDetection())
	t.Run("single client", func(t *testing.T) {
		m := &mockConn{}
		client, err := newSingleClient(&ClientOption{InitAddress: []string{""}}, m, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate Do Not Set", func(t *testing.T) {
			m.DoFn = func(cmd Completed) RedisResult {
				return newResult(RedisMessage{typ: ':', integer: 0}, nil)
			}
			if err := MSetNX(client, context.Background(), map[string]string{"1": "1", "2": "2"}); err["1"] != ErrMSetNXNotSet || err["2"] != ErrMSetNXNotSet {
				t.Fatalf("unexpected response %v", err)
			}
		})
	})
	t.Run("cluster client", func(t *testing.T) {
		m := &mockConn{
			DoFn: func(cmd Completed) RedisResult {
				return slotsResp
			},
		}
		client, err := newClusterClient(&ClientOption{InitAddress: []string{":0"}}, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate Do Not Set", func(t *testing.T) {
			m.DoFn = func(cmd Completed) RedisResult {
				return newResult(RedisMessage{typ: ':', integer: 0}, nil)
			}
			if err := MSetNX(client, context.Background(), map[string]string{"1": "1", "2": "2"}); err["1"] != ErrMSetNXNotSet || err["2"] != ErrMSetNXNotSet {
				t.Fatalf("unexpected response %v", err)
			}
		})
	})
}

//gocyclo:ignore
func TestJsonMGetCache(t *testing.T) {
	defer ShouldNotLeaked(SetupLeakDetection())
	t.Run("single client", func(t *testing.T) {
		m := &mockConn{}
		client, err := newSingleClient(&ClientOption{InitAddress: []string{""}}, m, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate DoMultiCache", func(t *testing.T) {
			m.DoMultiCacheFn = func(multi ...CacheableTTL) []RedisResult {
				result := make([]RedisResult, 0, len(multi))
				for _, cmd := range multi {
					if reflect.DeepEqual(cmd.Cmd.Commands(), []string{"JSON.GET", "1", "$"}) && cmd.TTL == 100 {
						result = append(result, newResult(RedisMessage{typ: '+', string: "1"}, nil))
					}
					if reflect.DeepEqual(cmd.Cmd.Commands(), []string{"JSON.GET", "2", "$"}) && cmd.TTL == 100 {
						result = append(result, newResult(RedisMessage{typ: '+', string: "2"}, nil))
					}
				}
				if len(result) != 2 {
					t.Fatalf("unexpected multi command %+v", multi)
				}

				return result
			}
			if v, err := JsonMGetCache(client, context.Background(), 100, []string{"1", "2"}, "$"); err != nil || v["1"].string != "1" || v["2"].string != "2" {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate DoCache Empty", func(t *testing.T) {
			if v, err := JsonMGetCache(client, context.Background(), 100, []string{}, "$"); err != nil || v == nil {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate DoCache Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoMultiCacheFn = func(multi ...CacheableTTL) []RedisResult {
				return []RedisResult{newResult(RedisMessage{}, context.Canceled), newResult(RedisMessage{}, context.Canceled)}
			}
			if v, err := JsonMGetCache(client, ctx, 100, []string{"1", "2"}, "$"); err != context.Canceled {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
	})
	t.Run("cluster client", func(t *testing.T) {
		m := &mockConn{
			DoFn: func(cmd Completed) RedisResult {
				return slotsResp
			},
		}
		client, err := newClusterClient(&ClientOption{InitAddress: []string{":0"}}, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate DoMultiCache", func(t *testing.T) {
			keys := make([]string, 100)
			for i := range keys {
				keys[i] = strconv.Itoa(i)
			}
			m.DoMultiCacheFn = func(multi ...CacheableTTL) []RedisResult {
				result := make([]RedisResult, 0, len(multi))
				for _, key := range keys {
					for _, cmd := range multi {
						if reflect.DeepEqual(cmd.Cmd.Commands(), []string{"JSON.GET", key, "$"}) && cmd.TTL == 100 {
							result = append(result, newResult(RedisMessage{typ: '+', string: key}, nil))
						}
					}
				}
				if len(result) != len(multi) {
					t.Fatalf("unexpected multi command: %+v", multi)
				}

				return result
			}
			v, err := JsonMGetCache(client, context.Background(), 100, keys, "$")
			if err != nil {
				t.Fatalf("unexpected response %v %v", v, err)
			}
			for _, key := range keys {
				if v[key].string != key {
					t.Fatalf("unexpected response %v", v)
				}
			}
		})
		t.Run("Delegate DoCache Empty", func(t *testing.T) {
			if v, err := JsonMGetCache(client, context.Background(), 100, []string{}, "$"); err != nil || v == nil {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate DoMultiCache Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoMultiCacheFn = func(multi ...CacheableTTL) []RedisResult {
				return []RedisResult{newResult(RedisMessage{}, context.Canceled), newResult(RedisMessage{}, context.Canceled)}
			}
			if v, err := JsonMGetCache(client, ctx, 100, []string{"1", "2"}, "$"); err != context.Canceled {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
	})
}

//gocyclo:ignore
func TestJsonMGet(t *testing.T) {
	defer ShouldNotLeaked(SetupLeakDetection())
	t.Run("single client", func(t *testing.T) {
		m := &mockConn{}
		client, err := newSingleClient(&ClientOption{InitAddress: []string{""}}, m, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate Do", func(t *testing.T) {
			m.DoFn = func(cmd Completed) RedisResult {
				if !reflect.DeepEqual(cmd.Commands(), []string{"JSON.MGET", "1", "2", "$"}) {
					t.Fatalf("unexpected command %v", cmd)
				}
				return newResult(RedisMessage{typ: '*', values: []RedisMessage{{typ: '+', string: "1"}, {typ: '+', string: "2"}}}, nil)
			}
			if v, err := JsonMGet(client, context.Background(), []string{"1", "2"}, "$"); err != nil || v["1"].string != "1" || v["2"].string != "2" {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate Do Empty", func(t *testing.T) {
			if v, err := JsonMGet(client, context.Background(), []string{}, "$"); err != nil || v == nil {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate Do Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoFn = func(cmd Completed) RedisResult {
				return newResult(RedisMessage{}, context.Canceled)
			}
			if v, err := JsonMGet(client, ctx, []string{"1", "2"}, "$"); err != context.Canceled {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
	})
	t.Run("cluster client", func(t *testing.T) {
		m := &mockConn{
			DoFn: func(cmd Completed) RedisResult {
				return slotsResp
			},
		}
		client, err := newClusterClient(&ClientOption{InitAddress: []string{":0"}}, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate DoMulti", func(t *testing.T) {
			keys := make([]string, 100)
			for i := range keys {
				keys[i] = strconv.Itoa(i)
			}
			m.DoMultiFn = func(multi ...Completed) []RedisResult {
				result := make([]RedisResult, 0, len(multi))
				for _, key := range keys {
					for _, cmd := range multi {
						if reflect.DeepEqual(cmd.Commands(), []string{"JSON.GET", key, "$"}) {
							result = append(result, newResult(RedisMessage{typ: '+', string: key}, nil))
						}
					}
				}
				if len(result) != len(multi) {
					t.Fatalf("unexpected multi command: %+v", multi)
				}

				return result
			}
			v, err := JsonMGet(client, context.Background(), keys, "$")
			if err != nil {
				t.Fatalf("unexpected response %v %v", v, err)
			}
			for _, key := range keys {
				if v[key].string != key {
					t.Fatalf("unexpected response %v", v)
				}
			}
		})
		t.Run("Delegate Do Empty", func(t *testing.T) {
			if v, err := JsonMGet(client, context.Background(), []string{}, "$"); err != nil || v == nil {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
		t.Run("Delegate DoMulti Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoMultiFn = func(multi ...Completed) []RedisResult {
				return []RedisResult{newResult(RedisMessage{}, context.Canceled), newResult(RedisMessage{}, context.Canceled)}
			}
			if v, err := JsonMGet(client, ctx, []string{"1", "2"}, "$"); err != context.Canceled {
				t.Fatalf("unexpected response %v %v", v, err)
			}
		})
	})
}

func TestJsonMSet(t *testing.T) {
	defer ShouldNotLeaked(SetupLeakDetection())
	t.Run("single client", func(t *testing.T) {
		m := &mockConn{}
		client, err := newSingleClient(&ClientOption{InitAddress: []string{""}}, m, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate Do", func(t *testing.T) {
			m.DoFn = func(cmd Completed) RedisResult {
				if !reflect.DeepEqual(cmd.Commands(), []string{"JSON.MSET", "1", "$", "1", "2", "$", "2"}) &&
					!reflect.DeepEqual(cmd.Commands(), []string{"JSON.MSET", "2", "$", "2", "1", "$", "1"}) {
					t.Fatalf("unexpected command %v", cmd)
				}
				return newResult(RedisMessage{typ: '+', string: "OK"}, nil)
			}
			if err := JsonMSet(client, context.Background(), map[string]string{"1": "1", "2": "2"}, "$"); err["1"] != nil || err["2"] != nil {
				t.Fatalf("unexpected response %v", err)
			}
		})
		t.Run("Delegate Do Empty", func(t *testing.T) {
			if err := JsonMSet(client, context.Background(), map[string]string{}, "$"); len(err) != 0 {
				t.Fatalf("unexpected response %v", err)
			}
		})
		t.Run("Delegate Do Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoFn = func(cmd Completed) RedisResult {
				return newResult(RedisMessage{}, context.Canceled)
			}
			if err := JsonMSet(client, ctx, map[string]string{"1": "1", "2": "2"}, "$"); err["1"] != context.Canceled || err["2"] != context.Canceled {
				t.Fatalf("unexpected response %v", err)
			}
		})
	})
	t.Run("cluster client", func(t *testing.T) {
		m := &mockConn{
			DoFn: func(cmd Completed) RedisResult {
				return slotsResp
			},
		}
		client, err := newClusterClient(&ClientOption{InitAddress: []string{":0"}}, func(dst string, opt *ClientOption) conn {
			return m
		})
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		t.Run("Delegate Do", func(t *testing.T) {
			keys := make(map[string]string, 100)
			for i := 0; i < 100; i++ {
				keys[strconv.Itoa(i)] = strconv.Itoa(i)
			}
			m.DoFn = func(cmd Completed) RedisResult {
				for key := range keys {
					if reflect.DeepEqual(cmd.Commands(), []string{"JSON.MSET", key, "$", key}) {
						return newResult(RedisMessage{typ: '+', string: "OK"}, nil)
					}
				}
				t.Fatalf("unexpected command %v", cmd)
				return RedisResult{}
			}
			err := JsonMSet(client, context.Background(), keys, "$")
			for key := range keys {
				if err[key] != nil {
					t.Fatalf("unexpected response %v", err)
				}
			}
		})
		t.Run("Delegate Do Empty", func(t *testing.T) {
			if err := JsonMSet(client, context.Background(), map[string]string{}, "$"); len(err) != 0 {
				t.Fatalf("unexpected response %v", err)
			}
		})
		t.Run("Delegate Do Err", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			m.DoFn = func(cmd Completed) RedisResult {
				return newResult(RedisMessage{}, context.Canceled)
			}
			if err := JsonMSet(client, ctx, map[string]string{"1": "1", "2": "2"}, "$"); err["1"] != context.Canceled || err["2"] != context.Canceled {
				t.Fatalf("unexpected response %v", err)
			}
		})
	})
}
