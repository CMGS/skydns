// Copyright (c) 2014 The SkyDNS Authors. All rights reserved.
// Use of this source code is governed by The MIT License (MIT) that can be
// found in the LICENSE file.

// Package etcd provides the default SkyDNS server Backend implementation,
// which looks up records stored under the `/skydns` key in etcd when queried.
package etcd

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/skynetservices/skydns/msg"
	"github.com/skynetservices/skydns/singleflight"

	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

const (
	DEFAULT_HOSTS = "default"
)

// Config represents configuration for the Etcd backend - these values
// should be taken directly from server.Config
type Config struct {
	Ttl      uint32
	Priority uint16
}

type Backend struct {
	client   etcd.KeysAPI
	ctx      context.Context
	config   *Config
	inflight *singleflight.Group
}

// NewBackend returns a new Backend for SkyDNS, backed by etcd.
func NewBackend(client etcd.KeysAPI, ctx context.Context, config *Config) *Backend {
	return &Backend{
		client:   client,
		ctx:      ctx,
		config:   config,
		inflight: &singleflight.Group{},
	}
}

func (g *Backend) Records(name string, exact bool, rmtIP net.IP) ([]msg.Service, error) {
	path, star := msg.PathWithWildcard(name)
	r, err := g.get(path, true)
	if err != nil {
		return nil, err
	}
	segments := strings.Split(msg.Path(name), "/")
	switch {
	case exact && r.Node.Dir:
		return nil, nil
	case r.Node.Dir:
		if _, err := g.get(fmt.Sprintf("%s/.wildcards", path), true); err != nil {
			return nil, err
		}
		if r2, err := g.get(fmt.Sprintf("%s/.self", path), true); err != nil {
			return g.loopNodes(r.Node.Nodes, segments, star, nil, rmtIP)
		} else {
			return g.loopNodes([]*etcd.Node{r2.Node}, segments, false, nil, rmtIP)
		}
	default:
		return g.loopNodes([]*etcd.Node{r.Node}, segments, false, nil, rmtIP)
	}
}

func (g *Backend) ReverseRecord(name string, rmtIP net.IP) (*msg.Service, error) {
	path, star := msg.PathWithWildcard(name)
	if star {
		return nil, fmt.Errorf("reverse can not contain wildcards")
	}
	r, err := g.get(path, true)
	if err != nil {
		return nil, err
	}
	if r.Node.Dir {
		return nil, fmt.Errorf("reverse must not be a directory")
	}
	segments := strings.Split(msg.Path(name), "/")
	records, err := g.loopNodes([]*etcd.Node{r.Node}, segments, false, nil, rmtIP)
	if err != nil {
		return nil, err
	}
	if len(records) != 1 {
		return nil, fmt.Errorf("must be only one service record")
	}
	return &records[0], nil
}

// get is a wrapper for client.Get that uses SingleInflight to suppress multiple
// outstanding queries.
func (g *Backend) get(path string, recursive bool) (*etcd.Response, error) {
	resp, err := g.inflight.Do(path, func() (interface{}, error) {
		r, e := g.client.Get(g.ctx, path, &etcd.GetOptions{Sort: false, Recursive: recursive})
		if e != nil {
			return nil, e
		}
		return r, e
	})
	if err != nil {
		return nil, err
	}
	return resp.(*etcd.Response), err
}

type bareService struct {
	Host     string
	Port     int
	Priority int
	Weight   int
	Text     string
}

// skydns/local/skydns/east/staging/web
// skydns/local/skydns/west/production/web
//
// skydns/local/skydns/*/*/web
// skydns/local/skydns/*/web

// loopNodes recursively loops through the nodes and returns all the values. The nodes' keyname
// will be match against any wildcards when star is true.
func (g *Backend) loopNodes(ns []*etcd.Node, nameParts []string, star bool, bx map[bareService]bool, rmtIP net.IP) (sx []msg.Service, err error) {
	if bx == nil {
		bx = make(map[bareService]bool)
	}
	var (
		defaultSx []msg.Service
		allSx     []msg.Service
	)
Nodes:
	for _, n := range ns {
		if strings.HasSuffix(n.Key, ".wildcards") {
			continue
		}
		if n.Dir {
			nodes, err := g.loopNodes(n.Nodes, nameParts, star, bx, rmtIP)
			if err != nil {
				return nil, err
			}
			sx = append(sx, nodes...)
			continue
		}
		if star {
			keyParts := strings.Split(n.Key, "/")
			for i, n := range nameParts {
				if i > len(keyParts)-1 {
					// name is longer than key
					continue Nodes
				}
				if n == "*" || n == "any" {
					continue
				}
				if keyParts[i] != n {
					continue Nodes
				}
			}
		}
		servs := map[string][]msg.Service{}
		if err := json.Unmarshal([]byte(n.Value), &servs); err != nil {
			return nil, err
		}

		if defaultList, ok := servs[DEFAULT_HOSTS]; ok {
			defaultSx = g.getServices(n, defaultList, bx)
			delete(servs, DEFAULT_HOSTS)
		}
		for cidr, servlist := range servs {
			sx = g.getServices(n, servlist, bx)
			if _, pool, err := net.ParseCIDR(cidr); err == nil && pool.Contains(rmtIP) {
				return sx, nil
			} else if err != nil {
				log.Printf("parse CIDR error %s", err)
				continue
			}
			allSx = append(allSx, sx...)
		}
	}
	if defaultSx != nil {
		return defaultSx, nil
	}
	return allSx, nil
}

func (g *Backend) getServices(n *etcd.Node, servlist []msg.Service, bx map[bareService]bool) (servset []msg.Service) {
	for _, serv := range servlist {
		b := bareService{serv.Host, serv.Port, serv.Priority, serv.Weight, serv.Text}
		if _, ok := bx[b]; ok {
			continue
		}
		bx[b] = true

		serv.Key = n.Key
		serv.Ttl = g.calculateTtl(n, &serv)
		if serv.Priority == 0 {
			serv.Priority = int(g.config.Priority)
		}
		servset = append(servset, serv)
	}
	return servset
}

// calculateTtl returns the smaller of the etcd TTL and the service's
// TTL. If neither of these are set (have a zero value), the server
// default is used.
func (g *Backend) calculateTtl(node *etcd.Node, serv *msg.Service) uint32 {
	etcdTtl := uint32(node.TTL)

	if etcdTtl == 0 && serv.Ttl == 0 {
		return g.config.Ttl
	}
	if etcdTtl == 0 {
		return serv.Ttl
	}
	if serv.Ttl == 0 {
		return etcdTtl
	}
	if etcdTtl < serv.Ttl {
		return etcdTtl
	}
	return serv.Ttl
}

// Client exposes the underlying Etcd client (used in tests).
func (g *Backend) Client() etcd.KeysAPI {
	return g.client
}
