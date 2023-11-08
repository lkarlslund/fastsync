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
			// logger.Trace().Msg("Ingestor locked")
			newitems := len(s.inchan) + 1

			if len(s.data) == 0 {
				// logger.Trace().Msg("Unblocking emitter")
				s.block.Done() // Unblock the emitter
			}

			// Grow by 2x size0
			if len(s.data)+newitems > cap(s.data) {
				newcap := cap(s.data) * 2
				if newcap == 0 {
					newcap = 4
				}
				for len(s.data)+newitems > newcap {
					newcap = newcap * 2
				}
				newdata := make([]T, len(s.data), newcap)
				copy(newdata, s.data)
				s.data = newdata
			}
			// logger.Trace().Msg("Adding first to stack")
			s.data = append(s.data, input)
			for len(s.inchan) > 0 {
				// logger.Trace().Msg("Adding another to stack")
				s.data = append(s.data, <-s.inchan)
			}
			s.lock.Unlock()
			// logger.Trace().Msg("Ingestor unlocked")
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
			// logger.Trace().Msg("Emitter waiting")
			s.block.Wait() // Wait for some data to show up
			// logger.Trace().Msg("Emitter done waiting")
			s.lock.Lock()
			// logger.Trace().Msg("Emitter locked")
			itemsoutput := 0
			for len(s.outchan) < cap(s.outchan) && len(s.data) > itemsoutput {
				// logger.Trace().Msg("Emitter outputting item")
				itemsoutput++
				output := s.data[len(s.data)-itemsoutput]
				s.outchan <- output
			}
			s.data = s.data[:len(s.data)-itemsoutput]
			if len(s.data) > 16 /* minimum size */ && len(s.data)*4 < cap(s.data) {
				// shrink it to len
				logger.Trace().Msgf("Shrinking stack from %v to %v", cap(s.data), len(s.data))
				newdata := make([]T, len(s.data))
				copy(newdata, s.data)
				s.data = newdata
			}
			if len(s.data) == 0 {
				// logger.Trace().Msg("Emitter blocking itself")
				s.block.Add(1) // Put ourselves to sleep
			}
			s.lock.Unlock()
			// logger.Trace().Msg("Emitter unlocked")
		}
		// logger.Trace().Msg("Emitter exiting, closing outchannel")
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
	return len(s.data) + len(s.inchan) + len(s.outchan)
}
