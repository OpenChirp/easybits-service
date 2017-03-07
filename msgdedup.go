package main

import (
	"sync"
	"time"
)

// type MD5 string

type MessageDedup struct {
	lock  sync.RWMutex
	check map[string]time.Time
}

func NewMessageDedup() *MessageDedup {
	md := new(MessageDedup)
	md.check = make(map[string]time.Time)
	return md
}

func (md *MessageDedup) Sending(topic string, data []byte) {

}
