package bytebufferpool

import (
	"sort"
	"sync"
	"sync/atomic"
)

const (
	minBitSize              = 6 //* 2**6=64 cpu三级缓存的cache line
	steps                   = 20
	minSize                 = 1 << minBitSize
	maxSize                 = 1 << (minBitSize + steps - 1)
	calibrateCallsThreshold = 42000
	maxPercentile           = 0.95
)

type Pool struct {
	calls       [steps]uint64 //*记录每个大小的对象被获取的次数
	calibrating uint64        //*是否正在校准
	defaultSize uint64        //*pool的大小
	maxSize     uint64        //*pool的最大大小
	pool        sync.Pool     //*对象池
}

var defaultPool Pool

func Get() *ByteBuffer {
	return defaultPool.Get()
}

func (p *Pool) Get() *ByteBuffer {
	v := p.pool.Get()
	if v != nil {
		//*将从对象池取出的接口类型对象转换成对应的类型
		return v.(*ByteBuffer)
	}
	return &ByteBuffer{
		B: make([]byte, p.defaultSize),
	}
}

func Put(b *ByteBuffer) { defaultPool.Put(b) }

// *将对象返回内存池
func (p *Pool) Put(b *ByteBuffer) {
	idx := index(len(b.B))

	if atomic.AddUint64(&p.calls[idx], 1) > calibrateCallsThreshold {
		p.calibrate()
	}

	maxSize := int(atomic.LoadUint64(&p.maxSize))
	if maxSize == 0 || cap(b.B) <= maxSize {
		b.Reset()
		p.pool.Put(b)
	}
}

func (p *Pool) calibrate() {
	if !atomic.CompareAndSwapUint64(&p.calibrating, 0, 1) {
		return
	}
	a := make(callSizes, 0, steps)
	var callsSum uint64
	for i := uint64(0); i < steps; i++ {
		calls := atomic.SwapUint64(&p.calls[i], 0)
		callsSum += calls

		a = append(a, callSize{calls: calls, size: minSize << i})

	}
	sort.Sort(a)

	defaultSize := a[0].size
	maxSize := defaultSize

	maxSum := uint64(float64(callsSum) * maxPercentile)
	callsSum = 0
	for i := 0; i < steps; i++ {
		if callsSum > maxSum {
			break
		}
		callsSum += a[i].calls
		size := a[i].size
		if size > maxSize {
			maxSize = size
		}
	}

	atomic.StoreUint64(&p.defaultSize, defaultSize)
	atomic.StoreUint64(&p.maxSize, maxSize)

	atomic.StoreUint64(&p.calibrating, 0)
}

type callSize struct {
	calls uint64
	size  uint64
}

type callSizes []callSize

// *返回长度
func (ci callSizes) Len() int {
	return len(ci)
}

// *比较大小return ci[i].calls>ci[j].calls
func (ci callSizes) Less(i, j int) bool {
	return ci[i].calls > ci[j].calls
}

// *交换元素
func (ci callSizes) Swap(i, j int) {
	ci[i], ci[j] = ci[j], ci[i]
}

// *是根据输入的整数 n 计算一个索引值 idx，该索引值反映了 n 的大小
func index(n int) int {
	n--
	n >>= minBitSize
	idx := 0
	for n > 0 {
		n >>= 1
		idx++
	}
	if idx >= steps {
		idx = steps - 1
	}
	return idx
}
