package workers

import (
	"fmt"
	"testing"
	"time"
)

const defaultManagerAddress = "127.0.0.1:1234"

// Some fake work
type Foo struct {
	Key string
}

type Args struct {
	String string
}

type Result struct {
	Result string
	Key    string
}

func (f Foo) Bar(args Args, result *Result) error {
	//	  time.Sleep(time.Millisecond * 1)
	result.Result = args.String
	result.Key = f.Key
	return nil
}

// A worker function.
func DoWorker(t *testing.T, managerAddr string, key string, done chan bool) *Worker {
	w := NewWorker()
	w.Register(Foo{key})
	go func() {
		err := w.Connect(managerAddr)
		if err != nil {
			t.Errorf("Error connecting: %v", err)
		}
		done <- true
	}()
	return w
}

// A manager.
func DoManager(t *testing.T, addr string) *Manager {
	m := NewManager()
	go m.ListenAndServe(addr)
	return m
}

func TestWorkers(t *testing.T) {
	m := DoManager(t, defaultManagerAddress)
	time.Sleep(time.Millisecond)
	done := make(chan bool)
	w1 := DoWorker(t, defaultManagerAddress, "abc", done)
	w2 := DoWorker(t, defaultManagerAddress, "abc", done)

	for i := 0; i < 5; i++ {
		str := fmt.Sprintf("call %d", i)
		var reply Result
		err := m.Call("Foo.Bar", Args{str}, &reply)
		if err != nil {
			t.Errorf("Error calling %d: %v", i, err)
		}
		if reply.Result != str {
			t.Errorf("Call %d returned %s, not %s", i, reply.Result, str)
		}
		if reply.Key != "abc" {
			t.Errorf("Call %d has key %s", i, reply.Key)
		}
	}

	// Close the server and make sure the workers die.
	_, _ = w1, w2
	m.Close()
	<-done
	<-done
}
