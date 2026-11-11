// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"testing"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func TestPanickingEventHandlerFailsDispatch(t *testing.T) {
	cli := NewClient(&store.Device{}, waLog.Noop)
	if cli.dispatchEvent(&events.Connected{}) {
		t.Fatal("dispatch without handlers failed")
	}
	cli.AddEventHandler(func(any) { panic("handler bug") })
	if !cli.dispatchEvent(&events.Connected{}) {
		t.Fatal("a panicking handler was reported as successful")
	}
	// The handler lock must have been released by the failed dispatch.
	cli.RemoveEventHandlers()
}
