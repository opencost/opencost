package prom

import (
	"fmt"
	"strings"
)

type CommError struct {
	messages []string
}

func NewCommError(messages ...string) CommError {
	return CommError{messages: messages}
}

func (pce CommError) Error() string {
	return fmt.Sprintf("Prometheus communication error: %s", strings.Join(pce.messages, ": "))
}

func (pce CommError) Wrap(message string) CommError {
	pce.messages = append([]string{message}, pce.messages...)
	return pce
}
