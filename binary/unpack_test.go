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
	if _, err := newDecoder(deep).read(false); !errors.Is(err, ErrDecodeLimit) {
		t.Fatalf("deeply nested JIDs: %v", err)
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

func TestDecoderNodeLimit(t *testing.T) {
	node := []byte{token.List8, 1, token.Binary8, 1, 'a'}
	decoder := newDecoder(node)
	decoder.nodes = maxDecodedNodes - 1
	if _, err := decoder.readNode(); err != nil {
		t.Fatalf("node at limit: %v", err)
	}
	decoder.index = 0
	if _, err := decoder.readNode(); !errors.Is(err, ErrDecodeLimit) {
		t.Fatalf("node over limit: %v", err)
	}

	// One list holds at most 65535 children, so spread the nodes over two lists.
	children := make([]Node, 40000)
	for i := range children {
		children[i].Tag = "message"
	}
	frame := func(perList int) []byte {
		encoded, err := Marshal(Node{Tag: "root", Content: []Node{
			{Tag: "list", Content: children[:perList]},
			{Tag: "list", Content: children[:perList]},
		}})
		if err != nil {
			t.Fatal(err)
		}
		// Marshal includes the frame header, which Unmarshal does not consume.
		return encoded[1:]
	}
	if _, err := Unmarshal(frame(40000)); !errors.Is(err, ErrDecodeLimit) {
		t.Fatalf("frame with 80003 nodes: %v", err)
	}
	if decoded, err := Unmarshal(frame(30000)); err != nil {
		t.Fatalf("frame with 60003 nodes: %v", err)
	} else if len(decoded.GetChildren()[1].GetChildren()) != 30000 {
		t.Fatal("frame with 60003 nodes was not decoded completely")
	}
}

func TestDecoderWideListsWithoutChildren(t *testing.T) {
	// Each list claims many children but holds only the first one. The padding makes every
	// declared size plausible for the remaining input.
	data := []byte{token.List16, 0xff, 0xff}
	for range 60 {
		data = append(data, token.List8, 2, token.Binary8, 1, 'x', token.List16, 0xff, 0xff)
	}
	data = append(data, make([]byte, 2*65535)...)
	if _, err := newDecoder(data).read(false); err == nil {
		t.Fatal("accepted malformed nested wide lists")
	}
}

func TestDecoderEmptyAndValidLists(t *testing.T) {
	data := []byte{token.List8, 2, token.List8, 1, token.Binary8, 1, 'a', token.List8, 1, token.Binary8, 1, 'b'}
	value, err := newDecoder(data).read(false)
	if err != nil {
		t.Fatal(err)
	}
	nodes := value.([]Node)
	if len(nodes) != 2 || nodes[0].Tag != "a" || nodes[1].Tag != "b" {
		t.Fatalf("unexpected nodes: %#v", nodes)
	}
	value, err = newDecoder([]byte{token.List8, 0}).read(false)
	if nodes, ok := value.([]Node); err != nil || !ok || nodes == nil || len(nodes) != 0 {
		t.Fatalf("empty list decoded as %#v (%v)", value, err)
	}
}

func TestUnmarshalRejectsOversizedInput(t *testing.T) {
	if node, err := Unmarshal(make([]byte, MaxUnpackedFrameSize+1)); node != nil || !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized input: node=%v err=%v", node, err)
	}
}

func FuzzUnmarshal(f *testing.F) {
	valid, err := Marshal(Node{Tag: "message", Content: []Node{{Tag: "body", Content: []byte("text")}}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid[1:])
	f.Add([]byte{token.List16, 0xff, 0xff})
	f.Add([]byte{token.JIDPair, token.ListEmpty, token.ListEmpty})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		_, _ = Unmarshal(data)
	})
}
