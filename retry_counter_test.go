// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"strconv"
	"testing"
)

func TestRetryCountersAreBounded(t *testing.T) {
	counters := make(map[string]int)
	if bumpRetryCounter(counters, "message") != 1 || bumpRetryCounter(counters, "message") != 2 {
		t.Fatal("counter did not count")
	}
	for i := range 10 * maxRetryCounters {
		bumpRetryCounter(counters, strconv.Itoa(i))
		if len(counters) > maxRetryCounters {
			t.Fatalf("map grew to %d entries", len(counters))
		}
	}

	// A known message keeps counting while the map is full.
	clear(counters)
	for i := range maxRetryCounters {
		bumpRetryCounter(counters, strconv.Itoa(i))
	}
	if got := bumpRetryCounter(counters, "0"); got != 2 || len(counters) != maxRetryCounters {
		t.Fatalf("count %d with %d entries, want 2 with %d", got, len(counters), maxRetryCounters)
	}
	if got := bumpRetryCounter(counters, "new"); got != 1 || len(counters) != 1 {
		t.Fatalf("count %d with %d entries after the map was full, want 1 with 1", got, len(counters))
	}
}
