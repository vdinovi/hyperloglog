// Package hyperloglog implements the hyperloglog algorithm for approximating
// the cardinality of distinct items in a multiset.
//
// References:
// - https://en.wikipedia.org/wiki/HyperLogLog
// - https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
// - http://antirez.com/news/75
package hyperloglog

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
)

// A counter holds the approximate state of the multiset.
// Values are presented via the Add method and an approximation
// of the count is given by the Count method
type Counter struct {
	regs  []uint8
	m     uint64
	b     uint64
	alpha float64
	hash  func([]byte, []byte) int
	hbuf  []byte
}

const minNumRegisters = 16

var (
	errNumRegistersTooSmall = fmt.Errorf("numRegisters must be greater than %d", minNumRegisters)
)

// Returns a new HyperLogLog counter
func NewCounter(numRegisters uint32) (*Counter, error) {
	if numRegisters < minNumRegisters {
		return nil, errNumRegistersTooSmall
	}
	c := &Counter{
		m:    uint64(numRegisters),
		b:    uint64(math.Log2(float64(numRegisters))),
		hash: sha_256,
	}
	c.alpha = alpha(c.m)
	c.regs = make([]uint8, c.m)
	c.hbuf = make([]byte, 8)
	return c, nil
}

// Presents a value to the counter
func (c *Counter) Add(in []byte) {
	c.hash(c.hbuf, in)
	v := binary.NativeEndian.Uint64(c.hbuf[:8])
	i := v & ((1 << c.b) - 1)
	c.regs[i] = max(c.regs[i], numZeroes(v)+1)
}

// Returns an approximation of the count
func (c *Counter) Count() float64 {
	var z float64
	for _, reg := range c.regs {
		z += math.Pow(2, -float64(reg))
	}
	z = 1 / z
	e := c.alpha * float64(c.m*c.m) * z
	return c.correction(e)
}

func (c *Counter) correction(e float64) float64 {
	if e < (2.0/5)*float64(c.m) {
		// small range correction
		var v float64
		for _, reg := range c.regs {
			if reg == 0 {
				v += 1
			}
		}
		if v != 0 {
			e = float64(c.m) * math.Log10(float64(c.m)/v)
		}
	} else if e > (1.0/30)*(1<<32) {
		// large range correction
		e = -(1 << 32) * math.Log10(1-(e/(1<<32)))
	}
	return e
}

var errMergeCounterMismatch = fmt.Errorf("cannot merge incompatible counters")

// Merges two counters together into a new register
// Note that the counters must contain the same number of registers
func (c *Counter) Merge(other *Counter) (*Counter, error) {
	if other.m != c.m {
		return nil, errMergeCounterMismatch
	}
	merged, err := NewCounter(uint32(c.m))
	if err != nil {
		return nil, err
	}
	for i, reg := range c.regs {
		merged.regs[i] = max(reg, other.regs[i])
	}
	return merged, nil
}

// Returns the approximate error
func (c *Counter) Error() float64 {
	return 1.04 * math.Sqrt(float64(c.m))
}

func numZeroes(v uint64) (i uint8) {
	var mask uint64
	for i = 0; i < 64; i += 1 {
		mask = uint64(1<<63) >> i
		if v&mask != 0 {
			break
		}
	}
	return i
}

func sha_256(dst []byte, src []byte) (n int) {
	x := sha256.Sum256(src)
	for n = range dst {
		dst[n] = x[n]
	}
	return n
}

func alpha(m uint64) float64 {
	if m < 16 {
		panic(m)
	} else if m <= 32 {
		return 0.673
	} else if m <= 64 {
		return 0.697
	} else if m <= 128 {
		return 0.709
	} else {
		return 0.7213 / (1 + (1.079 / float64(m)))
	}

}
