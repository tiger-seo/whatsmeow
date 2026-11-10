// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package binary

import (
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"testing"

	"go.mau.fi/whatsmeow/binary/token"
)

func TestUnpackLimits(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, size := range []int{0, MaxUnpackedFrameSize, MaxUnpackedFrameSize + 1} {
			t.Run(fmtUnpackCase(compressed, size), func(t *testing.T) {
				original := bytes.Repeat([]byte("x"), size)
				frame := append([]byte{0}, original...)
				if compressed {
					var buf bytes.Buffer
					buf.WriteByte(2)
					writer := zlib.NewWriter(&buf)
					if _, err := writer.Write(original); err != nil {
						t.Fatal(err)
					}
					if err := writer.Close(); err != nil {
						t.Fatal(err)
					}
					frame = buf.Bytes()
				}
				got, err := Unpack(frame)
				if size > MaxUnpackedFrameSize {
					if !errors.Is(err, ErrFrameTooLarge) || got != nil {
						t.Fatalf("oversized frame: got %d bytes, %v", len(got), err)
					}
				} else if err != nil || !bytes.Equal(got, original) {
					t.Fatalf("unpack: got %d bytes, %v", len(got), err)
				}
			})
		}
	}
}

func TestDecoderRejectsMalformedInput(t *testing.T) {
	for _, data := range [][]byte{
		{token.Nibble8, 0x80},
		{token.JIDPair, token.ListEmpty, token.ListEmpty},
		{token.ADJID, 0, 0, token.ListEmpty},
		{token.List16, 0xff, 0xff},
	} {
		if _, err := newDecoder(data).read(false); err == nil {
			t.Fatalf("accepted malformed input %x", data)
		}
	}
	deep := append(bytes.Repeat([]byte{token.JIDPair}, 65), token.ListEmpty)
	if _, err := newDecoder(deep).read(false); err == nil {
		t.Fatal("accepted deeply nested JIDs")
	}
}

func fmtUnpackCase(compressed bool, size int) string {
	return fmt.Sprintf("compressed=%t/size=%d", compressed, size)
}

func TestUnpackInvalid(t *testing.T) {
	if _, err := Unpack(nil); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("empty frame: %v", err)
	}
	for _, frame := range [][]byte{{2}, {2, 1, 2, 3}} {
		if _, err := Unpack(frame); err == nil {
			t.Fatal("invalid zlib data accepted")
		}
	}
}
