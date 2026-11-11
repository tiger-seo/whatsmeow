// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/socket"
	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func lifetimeClient(t *testing.T) (*Client, <-chan struct{}) {
	t.Helper()
	frames := make(chan struct{}, 1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			if _, _, err = conn.Read(r.Context()); err != nil {
				return
			}
			frames <- struct{}{}
		}
	}))
	t.Cleanup(server.Close)
	fs := socket.NewFrameSocket(waLog.Noop, server.Client())
	fs.URL = "ws" + strings.TrimPrefix(server.URL, "http")
	fs.HTTPHeaders.Del("Origin")
	fs.Header = nil
	if err := fs.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	nh := socket.NewNoiseHandshake()
	nh.Start("test", nil)
	ns, err := nh.Finish(fs, func(context.Context, []byte) {}, func(context.Context, *socket.NoiseSocket, bool) {})
	if err != nil {
		fs.Close(0)
		t.Fatal(err)
	}
	t.Cleanup(func() { ns.Stop(true, false) })
	cli := NewClient(&store.Device{}, waLog.Noop)
	cli.socket = ns
	cli.isLoggedIn.Store(true)
	return cli, frames
}

func assertNoWaiters(t *testing.T, cli *Client) {
	t.Helper()
	cli.responseWaitersLock.Lock()
	defer cli.responseWaitersLock.Unlock()
	if n := len(cli.responseWaiters); n != 0 {
		t.Fatalf("retained %d response waiters", n)
	}
}

func awaitFrame(t *testing.T, frames <-chan struct{}) {
	t.Helper()
	select {
	case <-frames:
	case <-time.After(5 * time.Second):
		t.Fatal("request frame not sent")
	}
}

func awaitWriteComplete(t *testing.T, cli *Client, frames <-chan struct{}) {
	t.Helper()
	awaitFrame(t, frames)
	if err := cli.socket.SendFrame(context.Background(), []byte{1}); err != nil {
		t.Fatal(err)
	}
	awaitFrame(t, frames)
}

func TestRequestWaiterTimeouts(t *testing.T) {
	cli, frames := lifetimeClient(t)
	for i := 0; i < 30; i++ {
		_, err := cli.sendIQ(context.Background(), infoQuery{Namespace: "w:p", Type: iqGet, Timeout: time.Millisecond})
		if !errors.Is(err, ErrIQTimedOut) {
			t.Fatalf("IQ error: %v", err)
		}
		awaitFrame(t, frames)
		assertNoWaiters(t, cli)
		_, err = cli.retryFrame(context.Background(), "test", "retry", []byte{1}, xmlStreamEndNode, time.Millisecond)
		if !errors.Is(err, ErrIQTimedOut) {
			t.Fatalf("retry error: %v", err)
		}
		awaitFrame(t, frames)
		assertNoWaiters(t, cli)
	}
	old := KeepAliveResponseDeadline
	KeepAliveResponseDeadline = time.Millisecond
	t.Cleanup(func() { KeepAliveResponseDeadline = old })
	for i := 0; i < 30; i++ {
		if success, cont := cli.sendKeepAlive(context.Background()); success || !cont {
			t.Fatalf("keepalive = %v, %v", success, cont)
		}
		awaitFrame(t, frames)
		assertNoWaiters(t, cli)
	}
}

func TestRequestWaiterCancellationAndResponse(t *testing.T) {
	cli, frames := lifetimeClient(t)
	for _, kind := range []string{"iq", "retry", "keepalive"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				switch kind {
				case "iq":
					_, err := cli.sendIQ(ctx, infoQuery{ID: "iq", Namespace: "w:p", Type: iqGet})
					done <- err
				case "retry":
					_, err := cli.retryFrame(ctx, "test", "retry", []byte{1}, xmlStreamEndNode, 0)
					done <- err
				case "keepalive":
					_, cont := cli.sendKeepAlive(ctx)
					if cont {
						done <- errors.New("keepalive continued")
					} else {
						done <- ctx.Err()
					}
				}
			}()
			awaitWriteComplete(t, cli, frames)
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v", err)
			}
			assertNoWaiters(t, cli)
		})
	}
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := cli.sendIQ(ctx, infoQuery{ID: "race", Namespace: "w:p", Type: iqGet}); done <- err }()
		awaitWriteComplete(t, cli, frames)
		response := &waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": "race", "type": "result"}}
		delivered := make(chan struct{})
		go func() { cli.receiveResponse(context.Background(), response); close(delivered) }()
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("race: %v", err)
		}
		<-delivered
		assertNoWaiters(t, cli)
	}
}

func TestRequestWaiterSuccessfulAndAsync(t *testing.T) {
	cli, frames := lifetimeClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := cli.sendIQAsync(ctx, infoQuery{ID: "async", Namespace: "w:p", Type: iqGet, Timeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	awaitWriteComplete(t, cli, frames)
	cancel()
	time.Sleep(5 * time.Millisecond)
	result := &waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": "async", "type": "result"}}
	if !cli.receiveResponse(context.Background(), result) || <-ch != result {
		t.Fatal("generic async waiter did not remain registered")
	}
	assertNoWaiters(t, cli)

	done := make(chan *waBinary.Node, 1)
	errs := make(chan error, 1)
	go func() {
		resp, err := cli.retryFrame(context.Background(), "test", "retry", []byte{1}, xmlStreamEndNode, 0)
		done <- resp
		errs <- err
	}()
	awaitFrame(t, frames)
	select {
	case <-done:
		t.Fatal("zero-timeout retry returned before a response")
	case <-time.After(5 * time.Millisecond):
	}
	result = &waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"id": "retry"}}
	if !cli.receiveResponse(context.Background(), result) || <-done != result || <-errs != nil {
		t.Fatal("retry did not receive the response")
	}
	assertNoWaiters(t, cli)

	keepalive := make(chan bool, 1)
	go func() { success, cont := cli.sendKeepAlive(context.Background()); keepalive <- success && cont }()
	awaitFrame(t, frames)
	cli.responseWaitersLock.Lock()
	var id string
	for id = range cli.responseWaiters {
	}
	cli.responseWaitersLock.Unlock()
	if id == "" || !cli.receiveResponse(context.Background(), &waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": id}}) || !<-keepalive {
		t.Fatal("keepalive did not receive the response")
	}
	assertNoWaiters(t, cli)
}
