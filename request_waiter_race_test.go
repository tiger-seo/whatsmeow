// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"sync"
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
)

func TestCancelResponseOwnership(t *testing.T) {
	cli := &Client{responseWaiters: make(map[string]chan<- *waBinary.Node)}
	first := cli.waitResponse("id")
	second := cli.waitResponse("id")
	cli.cancelResponse("id", first)
	node := &waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"id": "id"}}
	if !cli.receiveResponse(context.Background(), node) || <-second != node {
		t.Fatal("cancellation removed the replacement waiter")
	}
	cli.cancelResponse("id", second)
	select {
	case _, ok := <-second:
		if !ok {
			t.Fatal("cancellation closed a delivered waiter")
		}
		t.Fatal("unexpected response")
	default:
	}
	third := cli.waitResponse("id")
	cli.cancelResponse("id", third)
	if _, ok := <-third; ok {
		t.Fatal("registered waiter was not closed")
	}
	cli.cancelResponse("id", third)
}

func TestCancelResponseAfterClear(t *testing.T) {
	cli := &Client{responseWaiters: make(map[string]chan<- *waBinary.Node)}
	waiter := cli.waitResponse("id")
	cli.clearResponseWaiters(xmlStreamEndNode)
	cli.cancelResponse("id", waiter)
	if got := <-waiter; got != xmlStreamEndNode {
		t.Fatalf("got %v, want disconnect node", got)
	}

	waiter = cli.waitResponse("id")
	waiter <- xmlStreamEndNode
	cli.clearResponseWaiters(xmlStreamEndNode)
	cli.cancelResponse("id", waiter)
}

func TestConcurrentResponseCancellation(t *testing.T) {
	cli := &Client{responseWaiters: make(map[string]chan<- *waBinary.Node)}
	node := &waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"id": "id"}}
	for i := 0; i < 10000; i++ {
		waiter := cli.waitResponse("id")
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			cli.receiveResponse(context.Background(), node)
		}()
		go func() {
			defer wg.Done()
			cli.cancelResponse("id", waiter)
		}()
		wg.Wait()
	}
}
