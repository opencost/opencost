package parser

import "fmt"

// tokenType is the type of the token value returned by the lexer.
type tokenType int

const (
	Eof tokenType = iota
	Comment
	OpenBracket
	CloseBracket
	OpenSquareBracket
	CloseSquareBracket
	Comma
	Equal
	String
	Literal
	Value
	Timestamp
)

var tokenTypes = []string{
	Eof:          "EOF",
	Comment:      "Comment",
	OpenBracket:  "OpenBracket",
	CloseBracket: "CloseBracket",
	Comma:        "Comma",
	Equal:        "Equal",
	String:       "String",
	Literal:      "Literal",
	Value:        "Value",
	Timestamp:    "Timestamp",
}

func (tt tokenType) String() string {
	return tokenTypes[tt]
}

type token struct {
	Type  tokenType
	Value string
}

func (t token) String() string {
	return fmt.Sprintf("%s:%s", t.Type, t.Value)
}
