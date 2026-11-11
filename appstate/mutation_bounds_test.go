// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appstate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/util/cbcutil"
	"google.golang.org/protobuf/proto"
)

func boundsMutation(op waServerSync.SyncdMutation_SyncdOperation, blob []byte) *waServerSync.SyncdMutation {
	return &waServerSync.SyncdMutation{Operation: op.Enum(), Record: &waServerSync.SyncdRecord{Value: &waServerSync.SyncdValue{Blob: blob}}}
}

func TestMutationHashBounds(t *testing.T) {
	for _, op := range []waServerSync.SyncdMutation_SyncdOperation{waServerSync.SyncdMutation_SET, waServerSync.SyncdMutation_REMOVE} {
		for _, size := range []int{0, 1, 31, 32, 33} {
			t.Run(op.String()+"/"+strconv.Itoa(size), func(t *testing.T) {
				blob := bytes.Repeat([]byte{0x7a}, size)
				mutation := boundsMutation(op, blob)
				patch := &waServerSync.SyncdPatch{Mutations: []*waServerSync.SyncdMutation{mutation}}
				mac, err := generatePatchMAC(patch, "test", []byte("key"), 1)
				if (err != nil) != (size < 32) {
					t.Fatalf("patch MAC error: %v", err)
				}
				if err == nil && !bytes.Equal(mac, concatAndHMAC(sha256.New, []byte("key"), nil, blob[size-32:], uint64ToBytes(1), []byte("test"))) {
					t.Fatal("incorrect patch MAC")
				}
				var state HashState
				_, err = state.updateHash(patch.Mutations, func([]byte, int) ([]byte, error) { return nil, nil })
				if (err != nil) != (op == waServerSync.SyncdMutation_SET && size < 32) {
					t.Fatalf("update hash error: %v", err)
				}
				if err != nil && state.Hash != ([128]byte{}) {
					t.Fatal("hash changed on error")
				}
			})
		}
	}
	if _, err := generatePatchMAC(&waServerSync.SyncdPatch{Mutations: []*waServerSync.SyncdMutation{nil}}, "test", nil, 0); err == nil {
		t.Fatal("nil mutation accepted")
	}
}

func TestDecodePatchesRejectsShortSnapshotValue(t *testing.T) {
	proc := &Processor{}
	initial := HashState{Version: 7}
	list := &PatchList{Snapshot: &waServerSync.SyncdSnapshot{Records: []*waServerSync.SyncdRecord{{Value: &waServerSync.SyncdValue{Blob: []byte("sensitive")}}}}}
	mutations, state, err := proc.DecodePatches(context.Background(), list, initial, true)
	if err == nil || !strings.Contains(err.Error(), "too short for a MAC") || strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mutations) != 0 || state.Hash != initial.Hash {
		t.Fatalf("malformed snapshot produced mutations or changed hash")
	}
}

func TestDecodeMutationBounds(t *testing.T) {
	keys := expandAppStateKeys(bytes.Repeat([]byte{1}, 32))
	proc := &Processor{keyCache: map[string]ExpandedAppStateKeys{"": keys}}
	for _, op := range []waServerSync.SyncdMutation_SyncdOperation{waServerSync.SyncdMutation_SET, waServerSync.SyncdMutation_REMOVE} {
		for _, size := range []int{0, 1, 31, 32, 47, 48, 49, 63, 64} {
			for _, validate := range []bool{false, true} {
				blob := bytes.Repeat([]byte{0x7a}, size)
				if validate && size >= 32 {
					copy(blob[size-32:], generateContentMAC(op, blob[:size-32], nil, keys.ValueMAC))
				}
				_, _, _, _, _, err := proc.decodeMutation(context.Background(), boundsMutation(op, blob), 0, validate)
				if err == nil {
					t.Fatalf("%s size %d validate %t unexpectedly succeeded", op, size, validate)
				}
				if size < 32 && !strings.Contains(err.Error(), "too short for a MAC") {
					t.Fatalf("unexpected error: %v", err)
				}
				if size >= 32 && size < 48 && !strings.Contains(err.Error(), "too short for an IV") {
					t.Fatalf("unexpected error: %v", err)
				}
				if strings.Contains(err.Error(), strings.Repeat("7A", 8)) {
					t.Fatal("error exposed value")
				}
			}
		}
		plaintext, err := proto.Marshal(&waSyncAction.SyncActionData{Index: []byte(`["test"]`)})
		if err != nil {
			t.Fatal(err)
		}
		content, err := cbcutil.Encrypt(keys.ValueEncryption, nil, plaintext)
		if err != nil {
			t.Fatal(err)
		}
		blob := append(content, generateContentMAC(op, content, nil, keys.ValueMAC)...)
		mutation := boundsMutation(op, blob)
		mutation.Record.Index = &waServerSync.SyncdIndex{Blob: concatAndHMAC(sha256.New, keys.Index, []byte(`["test"]`))}
		_, _, index, _, _, err := proc.decodeMutation(context.Background(), mutation, 0, true)
		if err != nil || len(index) != 1 || index[0] != "test" {
			t.Fatalf("valid mutation: index %v, error %v", index, err)
		}
		blob[len(blob)-1] ^= 1
		_, _, _, _, _, err = proc.decodeMutation(context.Background(), mutation, 0, true)
		if !errors.Is(err, ErrMismatchingContentMAC) {
			t.Fatalf("invalid MAC: %v", err)
		}
	}
}
