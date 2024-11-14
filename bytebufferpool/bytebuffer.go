package bytebufferpool

import "io"

//*byte数组
type ByteBuffer struct {
	B []byte
}

// *返回长度
func (b *ByteBuffer) Len() int {
	return len(b.B)
}

// *从IO流中读取数据放入bytebuffer
func (b *ByteBuffer) ReadFrom(r io.Reader) (int64, error) {
	p := b.B
	nStart := int64(len(p)) //*起始位置
	nMax := int64(cap(p))   //*容量
	n := nStart
	//*容量为0 则设置为64
	if nMax == 0 {
		nMax = 64
		p = make([]byte, nMax)
	} else {
		p = p[:nMax]
	}
	for {
		if n == nMax {
			//*二倍扩容
			nMax *= 2
			bNew := make([]byte, nMax)
			copy(bNew, p)
			p = bNew
		}
		nn, err := r.Read(p[n:])
		n += int64(nn)
		if err != nil {
			b.B = p[:n]
			n -= nStart
			//*读取到文件结尾
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}
	}
}

//*将buffer的数据写入到io流
func (b *ByteBuffer) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(b.B)
	return int64(n), err
}

// *返回字节数组
func (b *ByteBuffer) Bytes() []byte {
	return b.B
}

// *写入字节数组
func (b *ByteBuffer) wtite(p []byte) (int, error) {
	b.B = append(b.B, p...)
	return len(p), nil
}

// *写入单个字节
func (b *ByteBuffer) WriteByte(c byte) error {
	b.B = append(b.B, c)
	return nil
}

// *写入一个字符串
func (b *ByteBuffer) WriteString(s string) (int, error) {
	b.B = append(b.B, s...)
	return len(s), nil
}

// *用p的内存覆盖b的内存
func (b *ByteBuffer) Set(p []byte) {
	b.B = append(b.B[:0], p...)
}

// *同上
func (b *ByteBuffer) SetString(s string) {
	b.B = append(b.B[:0], s...)
}

// *返回string字符串
func (b *ByteBuffer) String() string {
	return string(b.B)
}

// *重置
func (b *ByteBuffer) Reset() {
	b.B = b.B[:0]
}
