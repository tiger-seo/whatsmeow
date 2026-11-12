// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waVnameCert"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

type blockingMessageNameStore struct {
	store.NoopStore
	started chan string
	release chan struct{}
}

func (s *blockingMessageNameStore) PutPushName(_ context.Context, _ types.JID, name string) (bool, string, error) {
	s.started <- "push:" + name
	<-s.release
	return false, "", nil
}

func (s *blockingMessageNameStore) PutBusinessName(_ context.Context, _ types.JID, name string) (bool, string, error) {
	s.started <- "business:" + name
	<-s.release
	return false, "", nil
}

func nameTestClient(contacts *blockingMessageNameStore) *Client {
	jid := types.NewJID("self", types.DefaultUserServer)
	cli := NewClient(&store.Device{ID: &jid, Contacts: contacts}, waLog.Noop)
	cli.SynchronousAck = true
	return cli
}

func nameTestNode(name string) waBinary.Node {
	return waBinary.Node{Tag: "message", Attrs: waBinary.Attrs{
		"from": types.NewJID("sender", types.NewsletterServer), "id": name, "t": "123", "notify": name,
	}}
}

func TestMessageNameDirectHandlerWaitsForUpdate(t *testing.T) {
	for _, kind := range []string{"push", "business"} {
		t.Run(kind, func(t *testing.T) {
			contacts := &blockingMessageNameStore{started: make(chan string, 1), release: make(chan struct{})}
			defer close(contacts.release)
			cli := nameTestClient(contacts)
			node := nameTestNode("name")
			if kind == "business" {
				delete(node.Attrs, "notify")
				name := "name"
				details, err := proto.Marshal(&waVnameCert.VerifiedNameCertificate_Details{VerifiedName: &name})
				if err != nil {
					t.Fatal(err)
				}
				cert, err := proto.Marshal(&waVnameCert.VerifiedNameCertificate{Details: details})
				if err != nil {
					t.Fatal(err)
				}
				node.Content = []waBinary.Node{{Tag: "verified_name", Content: cert}}
			}
			done := make(chan struct{})
			go func() { cli.DangerousInternals().HandleEncryptedMessage(context.Background(), &node); close(done) }()
			select {
			case got := <-contacts.started:
				if got != kind+":name" {
					t.Fatalf("unexpected update %q", got)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("update did not start")
			}
			select {
			case <-done:
				t.Fatal("handler returned while name update was blocked")
			case <-time.After(100 * time.Millisecond):
			}
			contacts.release <- struct{}{}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not finish")
			}
		})
	}
}
