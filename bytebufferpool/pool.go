package bytebufferpool

import (
	"sort"
	"sync"
	"sync/atomic"
)

const (
	minBitSize              = 6                             //* 2**6=64 cpu三级缓存的cache line
	steps                   = 20                            //*最小大小到最大大小之间的增长步数
	minSize                 = 1 << minBitSize               //*左移六位,64字节
	maxSize                 = 1 << (minBitSize + steps - 1) //*左移25位,32MB
	calibrateCallsThreshold = 42000                         //*校准的阈值
	maxPercentile           = 0.95                          //*百分比
)

type Pool struct {
	calls       [steps]uint64 //*记录每个大小的对象被获取的次数
	calibrating uint64        //*是否正在校准
	defaultSize uint64        //*pool的默认大小
	maxSize     uint64        //*pool的最大大小
	pool        sync.Pool     //*对象池
}

var defaultPool Pool

//*返回一个字节数组对象
func Get() *ByteBuffer {
	return defaultPool.Get()
}

//*返回一个字节数组对象
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

//*将字节数组对象放回对象池
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

//*校准
func (p *Pool) calibrate() {
	//*已经校准过
	if !atomic.CompareAndSwapUint64(&p.calibrating, 0, 1) {
		return
	}

	//*创建20个元素的数组
	a := make(callSizes, 0, steps)
	var callsSum uint64
	for i := uint64(0); i < steps; i++ {
		//*将p.calls[i] 置为0,并返回原来的值到calls
		calls := atomic.SwapUint64(&p.calls[i], 0)
		//*全部加起来
		callsSum += calls
		//*minSize<<i 左移i位
		a = append(a, callSize{calls: calls, size: minSize << i})

	}
	//*根据calls排序,降序
	sort.Sort(a)

	//*调用次数最多的size
	defaultSize := a[0].size
	maxSize := defaultSize
	//*计算95%的调用次数
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
