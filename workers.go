/*
Package workers implements a simple scheme for parcelling out work to other processes over
a network via RPC.

One process will be have a Manager, accepting connections from Workers. The manager makes
RPC calls to the workers, potentially load-balancing among them (not yet implemented).
*/
package workers

import (
	"container/heap"
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
	pending     int
	index       int
	done        chan bool
}

type work struct {
	Service string
	Args    interface{}
	Reply   interface{}
	Done    chan error
}

// The heap that will manage worker load balancing.
type workerHeap []*workerConn

func (h workerHeap) Less(i, j int) bool {
	return h[i].pending < h[j].pending
}

func (h *workerHeap) Len() int {
	return len(*h)
}

func (h workerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *workerHeap) Push(x interface{}) {
	w := x.(*workerConn)
	w.index = h.Len()
	*h = append(*h, w)
}

func (h *workerHeap) Pop() interface{} {
	a := *h
	n := len(a)
	if n == 0 {
		return nil
	}
	*h = a[:n-1]
	a[n-1].index = -1
	return a[n-1]
}

// workerPool manages a priority queue of workerConns.
// See http://concur.rspace.googlecode.com/hg/talk/concur.html#slide-49 for
// the basic idea.
// This implementation is more complicated, to allow Pops to wait nicely for
// worker connections to become available.
type workerPool struct {
	heap       *workerHeap
	popChan    chan *workerConn
	pushChan   chan *workerConn
	removeChan chan *workerConn
}

func (p *workerPool) Init() {
	var h workerHeap
	p.heap = &h
	heap.Init(p.heap)
	// These channels must be unbuffered, or else synchronization problems may ensue.
	p.popChan = make(chan *workerConn)
	p.pushChan = make(chan *workerConn)
	p.removeChan = make(chan *workerConn)
	go p.run()
}

func (p workerPool) PopWorker() *workerConn {
	return <-p.popChan
}

func (p workerPool) PushWorker(w *workerConn) {
	p.pushChan <- w
}

func (p workerPool) RemoveWorker(w *workerConn) {
	p.removeChan <- w
}

// run is the loop that processes requests to push/pop/remove workers.
// It seems kind of complicated, but this effectively creates a heap
// where pops on an empty heap will wait for something to be inserted.
func (p workerPool) run() {
	var top *workerConn
	for {
		if top != nil {
			select {
			case p.popChan <- top:
				if p.heap.Len() == 0 {
					top = nil
				} else {
					top, _ = heap.Pop(p.heap).(*workerConn)
				}
			case w := <-p.pushChan:
				if w.pending < top.pending {
					top, w = w, top
				}
				heap.Push(p.heap, w)
			case w := <-p.removeChan:
				if w == top {
					if p.heap.Len() == 0 {
						top = nil
					} else {
						top, _ = heap.Pop(p.heap).(*workerConn)
					}
				} else {
					if w.index >= 0 {
						heap.Remove(p.heap, w.index)
					}
				}
			}
		} else {
			select {
			case w := <-p.pushChan:
				top = w
			case <-p.removeChan:
				log.Printf("Removing from empty heap")
			}
		}
	}
}

// Manager manages worker connections and allocates units of work to them.
type Manager struct {
	in           chan work
	closing      chan struct{}
	pool         workerPool
	listener     net.Listener
	dispatchLock sync.Mutex
}

// NewManager creates a manager.
func NewManager() *Manager {
	m := new(Manager)
	m.in = make(chan work, 1)
	m.closing = make(chan struct{})
	m.pool.Init()

	go func() {
		for {
			select {
			case w := <-m.in:
				m.dispatch(w)
			case <-m.closing:
				return
			}
		}
	}()
	return m
}

// dispatch sends a unit of work to a worker. It does not
// wait for the result.
func (m *Manager) dispatch(w work) error {
	// We need dispatchLock so changes to Pending will be atomic.
	// It might be better to move that to the pool, but this is simpler.
	m.dispatchLock.Lock()
	defer m.dispatchLock.Unlock()
	worker := m.pool.PopWorker()
	if worker == nil {
		err := errors.New("No worker")
		w.Done <- err
		return err
	}
	worker.pending++
	m.pool.PushWorker(worker)
	go func() {
		err := worker.client.Call(w.Service, w.Args, w.Reply)
		w.Done <- err
		if err != nil {
			m.pool.RemoveWorker(worker)
		} else {
			m.dispatchLock.Lock()
			defer m.dispatchLock.Unlock()
			m.pool.RemoveWorker(worker)
			worker.pending--
			m.pool.PushWorker(worker)
		}
	}()
	return nil
}

// ServeConn serves a connection to a worker. It blocks until the worker hangs up,
// or some other error occurs, or until the manager is closed.
func (m *Manager) ServeConn(conn net.Conn) {
	wrapped := &wrappedConn{conn, nil}
	client := rpc.NewClient(wrapped)
	worker := &workerConn{client: client, wrappedConn: wrapped}
	wrapped.worker = worker
	closing := m.closing
	m.pool.PushWorker(worker)
	for {
		select {
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
