// SPDX-License-Identifier: MIT
// Copyright 2026 Joel Rosdahl

package main

import (
	"context"
	"encoding/hex"
	"net/url"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type storageClient struct {
	client      *redis.Client
	context     context.Context
	timeout     time.Duration
	baseURL     *url.URL
	prefix      string
	bearerToken string
	logger      *logger
	mu          sync.Mutex
}

func newStorageClient(cfg *config, logger *logger) (*storageClient, error) {
	username := cfg.URL.User.Username()
	password, _ := cfg.URL.User.Password()
	// ccache sends password only as username
	if username != "" && password == "" {
		password = username
		username = ""
	}
	var network string
	var addr string
	var dbstr string
	switch cfg.URL.Scheme {
	case "redis":
		network = "tcp"
		addr = cfg.URL.Host
		dbstr = path.Base(cfg.URL.Path)
	case "redis+unix":
		network = "unix"
		addr = cfg.URL.Path
		dbstr = cfg.URL.Query().Get("db")
	}
	db := 0
	if dbstr != "." && dbstr != "" {
		i, err := strconv.Atoi(dbstr)
		if err != nil {
			return nil, err
		}
		db = i
	}
	client := redis.NewClient(&redis.Options{
		Network:         network,
		Username:        username,
		Password:        password,
		Addr:            addr,
		DB:              db,
		ConnMaxIdleTime: 90 * time.Second,
	})

	return &storageClient{
		client:      client,
		context:     context.Background(),
		timeout:     10 * time.Second,
		baseURL:     cfg.URL,
		prefix:      "ccache",
		bearerToken: cfg.BearerToken,
		logger:      logger,
	}, nil
}

func (s *storageClient) keyToPath(key []byte) string {
	keyHex := hex.EncodeToString(key)

	return keyHex
}

func (s *storageClient) buildURL(key []byte) (string, error) {
	path := s.keyToPath(key)

	return s.prefix + ":" + path, nil
}

func (s *storageClient) get(key []byte) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	urlStr, err := s.buildURL(key)
	if err != nil {
		return nil, false, err
	}

	s.logger.logf("GET %s", urlStr)
	ctx, cancel := context.WithTimeout(s.context, s.timeout)
	defer cancel()
	val, err := s.client.Get(ctx, urlStr).Result()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	return []byte(val), true, nil
}

func (s *storageClient) put(key []byte, value []byte, overwrite bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	urlStr, err := s.buildURL(key)
	if err != nil {
		return false, err
	}

	if !overwrite {
		exists, err := s.exists(urlStr)
		if err != nil {
			return false, err
		}
		if exists {
			return false, nil
		}
	}

	s.logger.logf("SET %s (%d bytes)", urlStr, len(value))
	ctx, cancel := context.WithTimeout(s.context, s.timeout)
	defer cancel()
	err = s.client.Set(ctx, urlStr, value, 0).Err()
	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *storageClient) remove(key []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	urlStr, err := s.buildURL(key)
	if err != nil {
		return false, err
	}

	s.logger.logf("DEL %s", urlStr)
	ctx, cancel := context.WithTimeout(s.context, s.timeout)
	defer cancel()
	val, err := s.client.Del(ctx, urlStr).Result()
	if err != nil {
		return false, err
	}

	return val != 0, nil
}

func (s *storageClient) exists(urlStr string) (bool, error) {
	ctx, cancel := context.WithTimeout(s.context, s.timeout)
	defer cancel()
	val, err := s.client.Exists(ctx, urlStr).Result()
	if err != nil {
		return false, err
	}

	return val != 0, nil
}
