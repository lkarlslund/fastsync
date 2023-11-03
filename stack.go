package main

import "sync"

type stack[T any] struct {
	outchan chan T
	inchan  chan T
	data    []T
	closed  bool
	lock    sync.Mutex
	block   sync.WaitGroup
}

func NewStack[T any]() (*stack[T], <-chan T, chan<- T) {
	s := &stack[T]{
		outchan: make(chan T),
		inchan:  make(chan T),
	}

	// ingestor
	go func() {
		for input := range s.inchan {
			s.lock.Lock()
			// Grow by 2x size
			if len(s.data) == cap(s.data) {
				newcap := cap(s.data) * 2
				if newcap < 4 {
					newcap = 4
				}
				newdata := make([]T, len(s.data), newcap)
				copy(newdata, s.data)
				s.data = newdata
			}
			s.data = append(s.data, input)
			if len(s.data) == 1 {
				s.block.Done() // Unblock the emitter
			}
			s.lock.Unlock()
		}
		s.closed = true
		s.lock.Lock()
		if len(s.data) == 0 {
			s.block.Done() // Unblock the emitter so it can exit
		}
		s.lock.Unlock()
	}()

	// emitter
	go func() {
		for {
			s.block.Wait() // Wait for some data to show up
			if s.closed {
				break
			}
			for len(s.data) > 0 {
				s.lock.Lock()
				output := s.data[len(s.data)-1]
				if len(s.data)*4 < cap(s.data) {
					// shrink it to len
					newdata := make([]T, len(s.data))
					copy(newdata, s.data)
					s.data = newdata
				}
				s.data = s.data[:len(s.data)-1]
				s.lock.Unlock()
				s.outchan <- output
			}
			s.block.Add(1) // Put ourselves to sleep
		}
		close(s.outchan)
	}()
	return s, s.outchan, s.inchan
}

func (s *stack[T]) Close() {
	close(s.inchan)
}

func (s *stack[T]) Len() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return len(s.data)
}
