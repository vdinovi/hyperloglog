package hyperloglog_test

import (
	"math"
	"testing"

	"github.com/vdinovi/go/streams"
	"github.com/vdinovi/hyperloglog"
)

func TestCounter(t *testing.T) {
	c, err := hyperloglog.NewCounter(32)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	rd, err := streams.NewRandomStringReader(5, 10)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	stream, err := streams.NewWordStreamer(rd)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	count := 100_000
	for i := 0; i < count; i += 1 {
		select {
		case err = <-stream.Errors():
			t.Fatalf("unexpected error: %s", err)
		case word := <-stream.Words():
			c.Add([]byte(word))
		}
	}

	if closingCh := stream.Close(); closingCh != nil {
		for err := range closingCh {
			t.Errorf("unexpected error: %s", err)
		}
	}

	error99 := 3 * c.Error()
	actual := c.Count()
	if diff := math.Abs(actual - float64(count)); diff > error99 {
		t.Errorf("expected count to be near %d +/- %f but was %f (+/- %f)", count, error99, actual, diff)
	}
}
