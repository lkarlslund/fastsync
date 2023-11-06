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

func NewStack[T any](inbuffer, outbuffer int) (*stack[T], <-chan T, chan<- T) {
	s := &stack[T]{
		inchan:  make(chan T, inbuffer),
		outchan: make(chan T, outbuffer),
	}

	s.block.Add(1) // We start while being asleep

	// ingestor
	go func() {
		for input := range s.inchan {
			s.lock.Lock()
			newitems := len(s.inchan) + 1

			// Grow by 2x size
			if len(s.data)+newitems > cap(s.data) {
				newcap := cap(s.data) * 2
				for len(s.data)+newitems > newcap {
					newcap = newcap * 2
				}
				newdata := make([]T, len(s.data), newcap)
				copy(newdata, s.data)
				s.data = newdata
			}
			if len(s.data) == 0 {
				s.block.Done() // Unblock the emitter
			}
			s.data = append(s.data, input)
			for len(s.inchan) > 0 {
				s.data = append(s.data, <-s.inchan)
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
			if s.closed {
				break
			}
			s.block.Wait() // Wait for some data to show up
			s.lock.Lock()
			itemsoutput := 0
			for len(s.outchan) < cap(s.outchan) && len(s.data) > itemsoutput {
				itemsoutput++
				output := s.data[len(s.data)-itemsoutput]
				s.outchan <- output
			}
			s.data = s.data[:len(s.data)-itemsoutput]
			if len(s.data)*4 < cap(s.data) {
				// shrink it to len
				newdata := make([]T, len(s.data))
				copy(newdata, s.data)
				s.data = newdata
			}
			if len(s.data) == 0 {
				s.block.Add(1) // Put ourselves to sleep
			}
			s.lock.Unlock()
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
