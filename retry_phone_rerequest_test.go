// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// recordingLog keeps every formatted line so a test can see which paths ran.
type recordingLog struct {
	mu    *sync.Mutex
	lines *[]string
}

func newRecordingLog() recordingLog { return recordingLog{mu: new(sync.Mutex), lines: new([]string)} }

func (l recordingLog) add(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	*l.lines = append(*l.lines, fmt.Sprintf(msg, args...))
}
func (l recordingLog) Errorf(msg string, args ...any) { l.add(msg, args...) }
func (l recordingLog) Warnf(msg string, args ...any)  { l.add(msg, args...) }
func (l recordingLog) Infof(msg string, args ...any)  { l.add(msg, args...) }
func (l recordingLog) Debugf(msg string, args ...any) { l.add(msg, args...) }
func (l recordingLog) Sub(string) waLog.Logger        { return l }

func (l recordingLog) contains(part string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range *l.lines {
		if strings.Contains(line, part) {
			return true
		}
	}
	return false
}

// TestRetryReceiptDoesNotRerequestFromPhoneUnlessEnabled sends the first retry receipt for a message that failed to
// decrypt, with synchronous and asynchronous acks, while AutomaticMessageRerequestFromPhone is off. Anyone can send a
// stanza that fails to decrypt, so no request may go to the phone unless it was asked for. The test client has no
// session store, so an attempted peer send would fail the test.
func TestRetryReceiptDoesNotRerequestFromPhoneUnlessEnabled(t *testing.T) {
	for _, syncAck := range []bool{true, false} {
		t.Run(fmt.Sprintf("syncAck=%t", syncAck), func(t *testing.T) {
			own := types.NewJID("self", types.DefaultUserServer)
			log := newRecordingLog()
			cli := NewClient(&store.Device{ID: &own}, log)
			cli.SynchronousAck = syncAck

			sender := types.NewJID("stranger", types.DefaultUserServer)
			node := &waBinary.Node{Tag: "message", Attrs: waBinary.Attrs{"id": "MSG1", "from": sender, "t": "123"}}
			info := &types.MessageInfo{MessageSource: types.MessageSource{Chat: sender, Sender: sender}, ID: "MSG1"}
			cli.sendRetryReceipt(context.Background(), node, info, false)

			// The retry receipt itself is attempted; there is no socket, so it fails.
			if !log.contains("Failed to send retry receipt for MSG1") {
				t.Fatal("the retry receipt was not attempted")
			}
			if log.contains("unavailable message MSG1") || log.contains("Requested message MSG1") {
				t.Fatal("the message was requested from the phone although the setting is off")
			}
		})
	}
}

func TestPhoneRerequestEnabled(t *testing.T) {
	cli := NewClient(&store.Device{}, waLog.Noop)
	if cli.phoneRerequestEnabled() {
		t.Fatal("enabled by default")
	}
	cli.AutomaticMessageRerequestFromPhone = true
	if !cli.phoneRerequestEnabled() {
		t.Fatal("not enabled by the setting")
	}
	cli.MessengerConfig = &MessengerConfig{}
	if cli.phoneRerequestEnabled() {
		t.Fatal("enabled with a messenger config")
	}
}
