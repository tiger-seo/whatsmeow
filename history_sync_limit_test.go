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
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func inlineHistorySync(t *testing.T, raw []byte) *waE2E.HistorySyncNotification {
	t.Helper()
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	if _, err := writer.Write(raw); err != nil {
		t.Fatal(err)
	} else if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	return &waE2E.HistorySyncNotification{InitialHistBootstrapInlinePayload: compressed.Bytes()}
}

func TestHistorySyncExpansionLimit(t *testing.T) {
	cli := &Client{Log: waLog.Noop, Store: &store.Device{}}

	// About 130 KiB of compressed zeros expand past the limit.
	notif := inlineHistorySync(t, make([]byte, MaxHistorySyncSize+1))
	if len(notif.InitialHistBootstrapInlinePayload) > 1<<20 {
		t.Fatalf("fixture is %d bytes compressed", len(notif.InitialHistBootstrapInlinePayload))
	}
	if _, err := cli.DownloadHistorySync(context.Background(), notif, true); !errors.Is(err, ErrHistorySyncTooLarge) {
		t.Fatalf("oversized history sync: %v", err)
	}

	raw, err := proto.Marshal(&waHistorySync.HistorySync{
		SyncType:   waHistorySync.HistorySync_RECENT.Enum(),
		ChunkOrder: proto.Uint32(3),
	})
	if err != nil {
		t.Fatal(err)
	}
	blob, err := cli.DownloadHistorySync(context.Background(), inlineHistorySync(t, raw), true)
	if err != nil {
		t.Fatalf("small history sync: %v", err)
	} else if blob.GetChunkOrder() != 3 {
		t.Fatalf("decoded chunk order %d", blob.GetChunkOrder())
	}
}
