package fastsync

import (
	"sync"
	"sync/atomic"
)

type stack[T any] struct {
	outchan chan T
	inchan  chan T
	data    []T
	closed  atomic.Bool
	lock    sync.Mutex
}

func NewStack[T any](inbuffer, outbuffer int) (*stack[T], <-chan T, chan<- T) {
	s := &stack[T]{
		inchan:  make(chan T, inbuffer),
		outchan: make(chan T, outbuffer),
	}

	// A single worker owns movement between the input, stack buffer, and output.
	go func() {
		defer close(s.outchan)

		for {
			s.lock.Lock()
			var output T
			var outputCh chan T
			if n := len(s.data); n > 0 {
				output = s.data[n-1]
				outputCh = s.outchan
			}
			s.lock.Unlock()

			select {
			case input, ok := <-s.inchan:
				if !ok {
					s.closed.Store(true)
					for {
						s.lock.Lock()
						if len(s.data) == 0 {
							s.lock.Unlock()
							return
						}
						last := len(s.data) - 1
						output := s.data[last]
						s.data = s.data[:last]
						s.lock.Unlock()
						s.outchan <- output
					}
				}
				s.lock.Lock()
				s.data = append(s.data, input)
				s.lock.Unlock()
			case outputCh <- output:
				s.lock.Lock()
				s.data = s.data[:len(s.data)-1]
				if len(s.data) > 16 && len(s.data)*4 < cap(s.data) {
					newdata := make([]T, len(s.data))
					copy(newdata, s.data)
					s.data = newdata
				}
				s.lock.Unlock()
			}
		}
	}()
	return s, s.outchan, s.inchan
}

func (s *stack[T]) Close() {
	close(s.inchan)
}

func (s *stack[T]) Len() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return len(s.data) + len(s.inchan) + len(s.outchan)
}
