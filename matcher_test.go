package main

import (
	"testing"
)

func TestBreaker(t *testing.T) {
	if 5 == 6 {
		t.Errorf("5 is not %d", 6)
	}
}
