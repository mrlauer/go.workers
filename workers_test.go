package workers

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

const defaultManagerAddress = "127.0.0.1:1234"

// Some fake work
type Foo struct {
	Key int
}

type Args struct {
	String string
}

type Result struct {
	Result string
	Key    int
}

func (f Foo) Bar(args Args, result *Result) error {
	time.Sleep(time.Millisecond * 1)
	result.Result = args.String
	result.Key = f.Key
	return nil
}

// A worker function.
func DoWorker(t *testing.T, managerAddr string, key int, wg *sync.WaitGroup) *Worker {
	w := NewWorker()
	w.Register(Foo{key})
	go func() {
		err := w.Connect(managerAddr)
		if err != nil {
			t.Errorf("Error connecting: %v", err)
		}
		wg.Done()
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
	nworkers := 10
	callfac := 10
	ncalls := nworkers * callfac

	m := DoManager(t, defaultManagerAddress)
	time.Sleep(time.Millisecond)
	wg := new(sync.WaitGroup)
	for i := 0; i < nworkers; i++ {
		wg.Add(1)
		DoWorker(t, defaultManagerAddress, i, wg)
	}

	keycount := make(map[int]int)
	var kclock sync.Mutex
	var callgroup sync.WaitGroup
	callgroup.Add(ncalls)
	for i := 0; i < ncalls; i++ {
		go func(i int) {
			str := fmt.Sprintf("call %d", i)
			var reply Result
			err := m.Call("Foo.Bar", Args{str}, &reply)
			if err != nil {
				t.Errorf("Error calling %d: %v", i, err)
			}
			if reply.Result != str {
				t.Errorf("Call %d returned %s, not %s", i, reply.Result, str)
			}
			kclock.Lock()
			keycount[reply.Key] += 1
			kclock.Unlock()
			callgroup.Done()
		}(i)
	}
	callgroup.Wait()
	// Check keys
	if len(keycount) != nworkers {
		t.Errorf("calls went to %d workers, not %d", len(keycount), nworkers)
	}
	for k, ct := range keycount {
		if ct != callfac {
			t.Errorf("key %d had %d calls", k, ct)
		}
	}

	// Close the server and make sure the workers die.
	m.Close()
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("Timeout waiting for workers to die")
	}
}
