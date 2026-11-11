// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"bytes"
	"compress/zlib"
	"context"
	"runtime"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func marshalFrame(t *testing.T, node waBinary.Node) []byte {
	t.Helper()
	data, err := waBinary.Marshal(node)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func newReceiveConn(generation uint64) (*receiveConn, *int) {
	closed := new(int)
	return &receiveConn{generation: generation, close: func() { *closed++ }}, closed
}

func TestFullHandlerQueueClosesConnection(t *testing.T) {
	cli := NewClient(&store.Device{}, waLog.Noop)
	cli.nodeHandlers["test"] = func(context.Context, *waBinary.Node) {}
	ctx := context.Background()
	queue := make(chan *waBinary.Node, 2)
	conn, closed := newReceiveConn(7)
	frame := marshalFrame(t, waBinary.Node{Tag: "test"})

	cli.handleConnFrame(ctx, ctx, frame, queue, conn)
	cli.handleConnFrame(ctx, ctx, frame, queue, conn)
	if _, ok := cli.LatestReceiveQueueOverload(); ok || *closed != 0 {
		t.Fatalf("connection closed before the queue was full")
	}

	goroutines := runtime.NumGoroutine()
	for range 100 {
		cli.handleConnFrame(ctx, ctx, frame, queue, conn)
	}
	if *closed != 1 {
		t.Errorf("connection closed %d times, want once", *closed)
	}
	if len(queue) != 2 {
		t.Errorf("queue holds %d nodes, want 2", len(queue))
	}
	if after := runtime.NumGoroutine(); after > goroutines {
		t.Errorf("overflow started goroutines: %d before, %d after", goroutines, after)
	}
	overload, ok := cli.LatestReceiveQueueOverload()
	if !ok || overload != (ReceiveQueueOverload{Generation: 7, Reason: ReceiveQueueItemLimit}) {
		t.Errorf("unexpected overload record %+v (found: %v)", overload, ok)
	}
}

func TestResponsesBypassFullHandlerQueue(t *testing.T) {
	cli := NewClient(&store.Device{}, waLog.Noop)
	cli.nodeHandlers["test"] = func(context.Context, *waBinary.Node) {}
	ctx := context.Background()
	queue := make(chan *waBinary.Node, 1)
	conn, closed := newReceiveConn(1)
	cli.handleConnFrame(ctx, ctx, marshalFrame(t, waBinary.Node{Tag: "test"}), queue, conn)

	response := cli.waitResponse("request")
	cli.handleConnFrame(ctx, ctx, marshalFrame(t, waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": "request"}}), queue, conn)
	select {
	case node := <-response:
		if node.Tag != "iq" {
			t.Errorf("unexpected response %s", node.Tag)
		}
	default:
		t.Error("response was not delivered while the queue was full")
	}
	if *closed != 0 {
		t.Error("a response closed the connection")
	}
}

func TestStreamErrorIsHandledWhenQueueIsFull(t *testing.T) {
	cli := NewClient(&store.Device{}, waLog.Noop)
	cli.nodeHandlers["test"] = func(context.Context, *waBinary.Node) {}
	replaced := make(chan struct{})
	cli.AddEventHandler(func(evt any) {
		if _, ok := evt.(*events.StreamReplaced); ok {
			close(replaced)
		}
	})
	ctx := context.Background()
	queue := make(chan *waBinary.Node, 1)
	conn, closed := newReceiveConn(1)
	cli.handleConnFrame(ctx, ctx, marshalFrame(t, waBinary.Node{Tag: "test"}), queue, conn)

	cli.handleConnFrame(ctx, ctx, marshalFrame(t, waBinary.Node{
		Tag:     "stream:error",
		Content: []waBinary.Node{{Tag: "conflict", Attrs: waBinary.Attrs{"type": "replaced"}}},
	}), queue, conn)
	select {
	case <-replaced:
	case <-time.After(5 * time.Second):
		t.Fatal("stream error was dropped")
	}
	if *closed != 1 {
		t.Errorf("connection closed %d times, want once", *closed)
	}
}

func TestOversizedFrameClosesConnection(t *testing.T) {
	var compressed bytes.Buffer
	compressed.WriteByte(2)
	writer := zlib.NewWriter(&compressed)
	if _, err := writer.Write(make([]byte, waBinary.MaxUnpackedFrameSize+1)); err != nil {
		t.Fatal(err)
	} else if err = writer.Close(); err != nil {
		t.Fatal(err)
	}

	cli := NewClient(&store.Device{}, waLog.Noop)
	ctx := context.Background()
	conn, closed := newReceiveConn(3)
	cli.handleConnFrame(ctx, ctx, compressed.Bytes(), make(chan *waBinary.Node, 1), conn)
	overload, ok := cli.LatestReceiveQueueOverload()
	if *closed != 1 || !ok || overload != (ReceiveQueueOverload{Generation: 3, Reason: ReceiveFrameSizeLimit}) {
		t.Errorf("closed %d times, overload record %+v (found: %v)", *closed, overload, ok)
	}
}

func TestDecodeLimitClosesConnection(t *testing.T) {
	// One list holds at most 65535 children, so spread the nodes over two lists.
	list := waBinary.Node{Tag: "list", Content: make([]waBinary.Node, 40000)}
	for i := range list.Content.([]waBinary.Node) {
		list.Content.([]waBinary.Node)[i].Tag = "message"
	}
	cli := NewClient(&store.Device{}, waLog.Noop)
	ctx := context.Background()
	conn, closed := newReceiveConn(2)
	cli.handleConnFrame(ctx, ctx, marshalFrame(t, waBinary.Node{Tag: "root", Content: []waBinary.Node{list, list}}), make(chan *waBinary.Node, 1), conn)
	overload, ok := cli.LatestReceiveQueueOverload()
	if *closed != 1 || !ok || overload != (ReceiveQueueOverload{Generation: 2, Reason: ReceiveDecodeLimit}) {
		t.Errorf("closed %d times, overload record %+v (found: %v)", *closed, overload, ok)
	}
}

func TestOverloadRecordKeepsNewestGeneration(t *testing.T) {
	cli := NewClient(&store.Device{}, waLog.Noop)
	newer, _ := newReceiveConn(5)
	older, olderClosed := newReceiveConn(4)
	cli.closeOverloadedConn(newer, ReceiveQueueItemLimit)
	cli.closeOverloadedConn(older, ReceiveFrameSizeLimit)
	if *olderClosed != 1 {
		t.Error("older connection was not closed")
	}
	if overload, _ := cli.LatestReceiveQueueOverload(); overload.Generation != 5 {
		t.Errorf("record shows generation %d, want 5", overload.Generation)
	}
	// A frame handler without a connection only drops the node.
	cli.closeOverloadedConn(nil, ReceiveQueueItemLimit)
}
