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
			if len(s.data) > 0 {
				output := s.data[len(s.data)-1]
				s.data = s.data[:len(s.data)-1]
				if len(s.data) > 16 && len(s.data)*4 < cap(s.data) {
					Logger.Trace().Msgf("Shrinking stack from %v to %v", cap(s.data), len(s.data))
					newdata := make([]T, len(s.data))
					copy(newdata, s.data)
					s.data = newdata
				}
				s.lock.Unlock()
				s.outchan <- output
				continue
			}
			s.lock.Unlock()

			input, ok := <-s.inchan
			if !ok {
				s.closed.Store(true)
				return
			}

			s.lock.Lock()
			newitems := len(s.inchan) + 1
			if len(s.data)+newitems > cap(s.data) {
				newcap := cap(s.data) * 2
				if newcap == 0 {
					newcap = 4
				}
				for len(s.data)+newitems > newcap {
					newcap *= 2
				}
				newdata := make([]T, len(s.data), newcap)
				copy(newdata, s.data)
				s.data = newdata
			}
			s.data = append(s.data, input)
			for len(s.inchan) > 0 {
				s.data = append(s.data, <-s.inchan)
			}
			s.lock.Unlock()
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
