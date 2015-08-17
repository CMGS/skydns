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
	"sync"

	"github.com/coreos/go-etcd/etcd"
	"github.com/skynetservices/skydns/msg"
)

// Config represents configuration for the Etcd backend - these values
// should be taken directly from server.Config
type Config struct {
	Ttl      uint32
	Priority uint16
}

type Backend struct {
	client   *etcd.Client
	config   *Config
	inflight *etcdSingle
}

// NewBackend returns a new Backend for SkyDNS, backed by etcd.
func NewBackend(client *etcd.Client, config *Config) *Backend {
	return &Backend{
		client:   client,
		config:   config,
		inflight: new(etcdSingle),
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
			return g.loopNodes(&r.Node.Nodes, segments, star, nil, rmtIP)
		} else {
			return g.loopNodes(&etcd.Nodes{r2.Node}, segments, false, nil, rmtIP)
		}
	default:
		return g.loopNodes(&etcd.Nodes{r.Node}, segments, false, nil, rmtIP)
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
	records, err := g.loopNodes(&etcd.Nodes{r.Node}, segments, false, nil, rmtIP)
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
	resp, err, _ := g.inflight.Do(path, func() (*etcd.Response, error) {
		r, e := g.client.Get(path, false, recursive)
		if e != nil {
			return nil, e
		}
		return r, e
	})
	if err != nil {
		return resp, err
	}
	// shared?
	return resp, err
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
func (g *Backend) loopNodes(n *etcd.Nodes, nameParts []string, star bool, bx map[bareService]bool, rmtIP net.IP) (sx []msg.Service, err error) {
	if bx == nil {
		bx = make(map[bareService]bool)
	}
	var (
		default_sx []msg.Service
		all_sx     []msg.Service
	)
Nodes:
	for _, n := range *n {
		if strings.HasSuffix(n.Key, ".wildcards") {
			continue
		}
		if n.Dir {
			nodes, err := g.loopNodes(&n.Nodes, nameParts, star, bx, rmtIP)
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
		for cidr, servlist := range servs {
			if cidr != "default" {
				if _, pool, err := net.ParseCIDR(cidr); err == nil && pool.Contains(rmtIP) {
					sx = g.getServices(n, servlist, bx)
					return sx, nil
				} else if err != nil {
					log.Printf("parse CIDR error %s", err)
					continue
				}
			} else {
				default_sx = g.getServices(n, servlist, bx)
				continue
			}
			all_sx = append(all_sx, g.getServices(n, servlist, bx)...)
		}
	}
	if default_sx != nil {
		return default_sx, nil
	} else {
		return all_sx, nil
	}
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

// Client exposes the underlying Etcd client.
func (g *Backend) Client() *etcd.Client {
	return g.client
}

// UpdateClient allows the etcd client used by this backend
// to be replaced on the fly.
func (g *Backend) UpdateClient(client *etcd.Client) {
	g.client = client
}

type etcdCall struct {
	wg   sync.WaitGroup
	val  *etcd.Response
	err  error
	dups int
}

type etcdSingle struct {
	sync.Mutex
	m map[string]*etcdCall
}

func (g *etcdSingle) Do(key string, fn func() (*etcd.Response, error)) (*etcd.Response, error, bool) {
	g.Lock()
	if g.m == nil {
		g.m = make(map[string]*etcdCall)
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		g.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := new(etcdCall)
	c.wg.Add(1)
	g.m[key] = c
	g.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.Lock()
	delete(g.m, key)
	g.Unlock()

	return c.val, c.err, c.dups > 0
}
