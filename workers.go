package workers

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Worker process
// Dials a master, serves rpc
type Worker struct {
	server *rpc.Server
}

// NewWorker creates a worker.
func NewWorker() *Worker {
	server := rpc.NewServer()
	return &Worker{server: server}
}

// Register registers a type for RPC
func (w *Worker) Register(t interface{}) {
	w.server.Register(t)
}

// Connect connects to a manager process. It blocks.
func (w *Worker) Connect(managerAddr string) error {
	conn, err := net.Dial("tcp", managerAddr)
	if err != nil {
		return fmt.Errorf("Error dialing: %v", err)
	}
	w.server.ServeConn(conn)
	return nil
}

// Manager process
// Accepts connections
//	Keeps them in a list (a priority queue, for load balancing?)
// On each request, find a connection that's not currently active and send to it.

// wrappedConn is a thin wrapper around a connection. It closes its workerConn when the
// connection dies.
type wrappedConn struct {
	io.ReadWriteCloser
	worker *workerConn
}

func (w wrappedConn) Read(data []byte) (int, error) {
	n, err := w.ReadWriteCloser.Read(data)
	if err != nil {
		w.worker.client.Close()
	}
	return n, err
}

type workerConn struct {
	client      *rpc.Client
	wrappedConn *wrappedConn
}

type work struct {
	Args  interface{}
	Reply interface{}
	Done  chan error
}

// Manager manages worker connections and allocates units of work to them.
type Manager struct {
	in   chan work
	lock sync.Mutex
}

func NewManager() *Manager {
	m := new(Manager)
	m.in = make(chan work, 1)
	return m
}

// ServeConn serves a connection to a worker. It blocks until the worker hangs up
// or some other error occurs.
func (m *Manager) ServeConn(conn net.Conn) {
	wrapped := &wrappedConn{conn, nil}
	client := rpc.NewClient(wrapped)
	worker := &workerConn{client, wrapped}
	wrapped.worker = worker
	done := make(chan bool)
	for {
		select {
		case w, ok := <-m.in:
			if !ok {
				return
			}
			go func(w work) {
				log.Printf("Processing on %v\n", conn.RemoteAddr())
				err := client.Call("T.Doit", w.Args, w.Reply)
				w.Done <- err
				if err != nil {
					done <- true
				}
			}(w)
		case <-done:
			return
		}
	}
}

func (m *Manager) ProcessRequest(args interface{}, reply interface{}) error {
	done := make(chan error, 1)
	timeout := time.Second
	select {
	case m.in <- work{args, reply, done}:
		err := <-done
		return err
	case <-time.After(timeout):
		return errors.New("Timeout")
	}
	return nil
}

// ListenAndServe listens for worker connections and serves them.
// It blocks.
func (m *Manager) ListenAndServe(addr string) error {
	l, e := net.Listen("tcp", addr)
	if e != nil {
		return fmt.Errorf("Error listening: %v", e)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Accept error %T %v\n", err, err)
		} else {
			go m.ServeConn(conn)
		}
	}
	return nil
}

