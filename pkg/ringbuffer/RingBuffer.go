package ringbuffer

type RingBuffer[T any] struct {
	buffer   []T
	first    int
	length   int
	capacity int
}

func New[T any](capacity int) RingBuffer[T] {
	return RingBuffer[T]{
		buffer:   make([]T, capacity),
		first:    0,
		length:   0,
		capacity: capacity,
	}
}

func (rb *RingBuffer[T]) Get(i int) T {
	if i >= rb.capacity {
		panic(1)
	}
	absoluteNdx := rb.first + i
	if absoluteNdx >= rb.capacity {
		absoluteNdx = absoluteNdx - rb.capacity
	}
	return rb.buffer[absoluteNdx]
}

func (rb *RingBuffer[T]) Add(values ...T) {
	var v T
	for v = range values {
		rb.add(v)
	}
}

func (rb *RingBuffer[T]) add(value T) {
	if rb.length == rb.capacity {
		rb.buffer[rb.first] = value
		rb.first++
		if rb.first == rb.capacity {
			rb.first = 0
		}
	} else {
		rb.buffer[rb.length] = value
		rb.length++
	}
}

func (rb *RingBuffer[T]) Length() int {
	return rb.length
}
