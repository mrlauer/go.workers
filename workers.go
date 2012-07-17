/*
Package workers implements a simple scheme for parcelling out work to other processes over
a network via RPC.

One process will be have a Manager, accepting connections from Workers. The manager makes
RPC calls to the workers, potentially load-balancing among them (not yet implemented).
*/
package workers

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Worker process
// Dials a master, serves rpc
type Worker struct {
	server *rpc.Server
	conn   net.Conn
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

// Connect connects to a manager process. It blocks until the connection is closed, generally when
// the Manager shuts down.
func (w *Worker) Connect(managerAddr string) error {
	if w.conn != nil {
		return errors.New("Already connected")
	}
	conn, err := net.Dial("tcp", managerAddr)
	if err != nil {
		return fmt.Errorf("Error dialing: %v", err)
	}
	w.conn = conn
	w.server.ServeConn(conn)
	return nil
}

func (w *Worker) Close() {
	if w.conn != nil {
		w.conn.Close()
		w.conn = nil
	}
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
	Service string
	Args    interface{}
	Reply   interface{}
	Done    chan error
}

// Manager manages worker connections and allocates units of work to them.
type Manager struct {
	in       chan work
	closing  chan struct{}
	listener net.Listener
	lock     sync.Mutex
}

// NewManager creates a manager.
func NewManager() *Manager {
	m := new(Manager)
	m.in = make(chan work, 1)
	m.closing = make(chan struct{})
	return m
}

// ServeConn serves a connection to a worker. It blocks until the worker hangs up,
// or some other error occurs, or until the manager is closed.
func (m *Manager) ServeConn(conn net.Conn) {
	wrapped := &wrappedConn{conn, nil}
	client := rpc.NewClient(wrapped)
	worker := &workerConn{client, wrapped}
	wrapped.worker = worker
	done := make(chan bool)
	closing := m.closing
	for {
		select {
		case w, ok := <-m.in:
			if !ok {
				return
			}
			go func(w work) {
				err := client.Call(w.Service, w.Args, w.Reply)
				w.Done <- err
				if err != nil {
					done <- true
				}
			}(w)
		case <-done:
			return
		case <-closing:
			conn.Close()
			return
		}
	}
}

// Call forwards a service call to a worker.
func (m *Manager) Call(service string, args interface{}, reply interface{}) error {
	done := make(chan error, 1)
	timeout := time.Second
	select {
	case m.in <- work{service, args, reply, done}:
		err := <-done
		return err
	case <-time.After(timeout):
		return errors.New("Timeout")
	}
	return nil
}

// Close stops listening and closes all active connections.
func (m *Manager) Close() {
	if m.listener != nil {
		m.listener.Close()
		m.listener = nil
	}
	close(m.closing)
}

// ListenAndServe listens for worker connections and serves them.
// It blocks.
func (m *Manager) ListenAndServe(addr string) error {
	if m.listener != nil {
		return errors.New("Already listening")
	}
	l, e := net.Listen("tcp", addr)
	if e != nil {
		return fmt.Errorf("Error listening: %v", e)
	}
	m.listener = l
	for {
		conn, err := l.Accept()
		if err != nil {
			break
		} else {
			go m.ServeConn(conn)
		}
	}
	return nil
}
