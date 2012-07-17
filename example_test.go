package workers

import (
	"fmt"
	"log"
	"time"
)

type T struct {
}

func (t T) Echo(arg string, reply *string) error {
	*reply = arg
	time.Sleep(time.Millisecond * 2)
	return nil
}

func exampleWorker(managerAddr string) {
	w := NewWorker()
	w.Register(T{})
	go func() {
		err := w.Connect(managerAddr)
		if err != nil {
			log.Fatal(err)
		}
	}()
}

func exampleManager(managerAddr string) *Manager {
	m := NewManager()
	go m.ListenAndServe(managerAddr)
	return m
}

// In this example we'll start one manager and two workers
// in the same process.
func ExampleManager() {
	addr := "127.0.0.1:1234"
	m := exampleManager(addr)
	time.Sleep(time.Millisecond)
	exampleWorker(addr)
	exampleWorker(addr)
	for _, s := range []string{"foo", "bar", "baz"} {
		var reply string
		err := m.Call("T.Echo", s, &reply)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(reply)
	}
	m.Close()
	// Output:
	// foo
	// bar
	// baz
}
