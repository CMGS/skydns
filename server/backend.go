// Copyright (c) 2014 The SkyDNS Authors. All rights reserved.
// Use of this source code is governed by The MIT License (MIT) that can be
// found in the LICENSE file.

package server

import (
	"net"

	"github.com/skynetservices/skydns/msg"
)

type Backend interface {
	Records(name string, exact bool, rmtIP net.IP) ([]msg.Service, error)
	ReverseRecord(name string, rmtIP net.IP) (*msg.Service, error)
}

// FirstBackend exposes the Backend interface over multiple Backends, returning
// the first Backend that answers the provided record request. If no Backend answers
// a record request, the last error seen will be returned.
type FirstBackend []Backend

// FirstBackend implements Backend
var _ Backend = FirstBackend{}

func (g FirstBackend) Records(name string, exact bool, rmtIP net.IP) (records []msg.Service, err error) {
	var lastError error
	for _, backend := range g {
		if records, err = backend.Records(name, exact, rmtIP); err == nil && len(records) > 0 {
			return records, nil
		}
		if err != nil {
			lastError = err
		}
	}
	return nil, lastError
}

func (g FirstBackend) ReverseRecord(name string, rmtIP net.IP) (record *msg.Service, err error) {
	var lastError error
	for _, backend := range g {
		if record, err = backend.ReverseRecord(name, rmtIP); err == nil && record != nil {
			return record, nil
		}
		if err != nil {
			lastError = err
		}
	}
	return nil, lastError
}
