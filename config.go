// Copyright (c) 2014 The SkyDNS Authors. All rights reserved.
// Use of this source code is governed by The MIT License (MIT) that can be
// found in the LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/miekg/dns"
)

// Config provides options to the skydns resolver
type Config struct {
	DnsAddr      string        `json:"dns_addr,omitempty"`
	Domain       string        `json:"domain,omitempty"`
	DomainLabels int           `json:"-"`
	DnsSec       string        `json:"dnssec,omitempty"`
	RoundRobin   bool          `json:"round_robin,omitempty"`
	Nameservers  []string      `json:"nameservers,omitempty"`
	ReadTimeout  time.Duration `json:"read_timeout,omitempty"`
	WriteTimeout time.Duration `json:"write_timeout,omitempty"`

	// DNSSEC key material
	PubKey  *dns.DNSKEY    `json:"-"`
	KeyTag  uint16         `json:"-"`
	PrivKey dns.PrivateKey `json:"-"`
}

func LoadConfig(client *etcd.Client) (*Config, error) {
	n, err := client.Get("/skydns/config", false, false)
	if err != nil {
		return nil, err
	}
	var config *Config
	if err := json.Unmarshal([]byte(n.Node.Value), &config); err != nil {
		return nil, err
	}
	if err := setDefaults(config); err != nil {
		return nil, err
	}
	return config, nil
}

func setDefaults(config *Config) error {
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 2 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 2 * time.Second
	}
	if config.DnsAddr == "" {
		config.DnsAddr = "127.0.0.1:53"
	}
	if config.Domain == "" {
		config.Domain = "skydns.local"
	}

	if len(config.Nameservers) == 0 {
		c, err := dns.ClientConfigFromFile("/etc/resolv.conf")
		if err != nil {
			return err
		}
		for _, s := range c.Servers {
			config.Nameservers = append(config.Nameservers, net.JoinHostPort(s, c.Port))
		}
	}
	if config.DnsSec != "" {
		k, p, err := ParseKeyFile(config.DnsSec)
		if err != nil {
			return err
		}
		if k.Header().Name != dns.Fqdn(config.Domain) {
			return fmt.Errorf("ownername of DNSKEY must match SkyDNS domain")
		}
		config.PubKey = k
		config.KeyTag = k.KeyTag()
		config.PrivKey = p
	}
	config.Domain = dns.Fqdn(strings.ToLower(config.Domain))
	config.DomainLabels = dns.CountLabel(dns.Fqdn(config.Domain))

	return nil
}
