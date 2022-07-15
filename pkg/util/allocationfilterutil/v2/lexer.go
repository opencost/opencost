package allocationfilterutil

import (
	"fmt"

	multierror "github.com/hashicorp/go-multierror"

	"github.com/opencost/opencost/pkg/kubecost"
)

// ============================================================================
// This file contains:
// Lexing (string -> []token) for V2 of allocation filters
// ============================================================================
//
// See parser.go for a formal grammar and external links.

type tokenKind int

const (
	colon tokenKind = iota // ':'
	comma                  // ','
	plus                   // '+'

	bangColon // '!:'

	str // '"foo"'

	filterField1 // 'namespace', 'cluster'
	filterField2 // 'label', 'annotation'
	keyedAccess  // '[app]', '[foo]', etc.
	identifier   // K8s valid name + sanitized Prom: 'app', 'abc_label'

	eof
)

// These maps serve a dual purpose. (1) to help the lexer identify special
// strings that should become filterField1/2 instead of identifiers and (2) to
// help the parser convert tokens into AllocationFilterConditions.
var ff1ToKCFilterField = map[string]kubecost.FilterField{
	"cluster":        kubecost.FilterClusterID,
	"node":           kubecost.FilterNode,
	"namespace":      kubecost.FilterNamespace,
	"controllerName": kubecost.FilterControllerName,
	"controllerKind": kubecost.FilterControllerKind,
	"container":      kubecost.FilterContainer,
	"pod":            kubecost.FilterPod,
	"services":       kubecost.FilterServices,
}
var ff2ToKCFilterField = map[string]kubecost.FilterField{
	"label":      kubecost.FilterLabel,
	"annotation": kubecost.FilterAnnotation,
}

func (tk tokenKind) String() string {
	switch tk {
	case colon:
		return "colon"
	case comma:
		return "comma"
	case plus:
		return "plus"
	case bangColon:
		return "bangColon"
	case str:
		return "str"
	case filterField1:
		return "filterField1"
	case filterField2:
		return "filterField2"
	case keyedAccess:
		return "keyedAccess"
	case identifier:
		return "identifier"
	case eof:
		return "eof"
	default:
		return fmt.Sprintf("Unspecified: %d", tk)
	}
}

// ============================================================================
// Lexer/Scanner
//
// Based on the Scanner class in Chapter 4: Scanning of Crafting Interpreters by
// Robert Nystrom
// ============================================================================

type token struct {
	kind tokenKind
	s    string
}

func (t token) String() string {
	return fmt.Sprintf("%s:%s", t.kind, t.s)
}

type scanner struct {
	source string
	tokens []token
	errors []error

	lexemeStartByte int
	nextByte        int
}

func (s *scanner) scanTokens() {
	for !s.atEnd() {
		s.lexemeStartByte = s.nextByte
		s.scanToken()
	}

	s.tokens = append(s.tokens, token{kind: eof})
}

func (s scanner) atEnd() bool {
	return s.nextByte >= len(s.source)
}

// advance returns a byte because we only accept ASCII, which has to fit in a
// byte
//
// TODO: If we add unicode support, advance() will probably have to return a
// rune.
func (s *scanner) advance() byte {
	b := s.source[s.nextByte]
	s.nextByte += 1
	return b
}

func (s *scanner) match(expected byte) bool {
	if s.atEnd() {
		return false
	}
	if s.source[s.nextByte] != expected {
		return false
	}
	s.nextByte += 1
	return true
}

func (s *scanner) addToken(kind tokenKind) {
	lexemeString := s.source[s.lexemeStartByte:s.nextByte]
	switch kind {
	// Eliminate surrounding characters like " and []
	case str, keyedAccess:
		lexemeString = lexemeString[1 : len(lexemeString)-1]
	}

	s.tokens = append(s.tokens, token{
		kind: kind,
		s:    lexemeString,
	})
}

func (s *scanner) peek() byte {
	if s.atEnd() {
		return 0
	}
	return s.source[s.nextByte]
}

func (s *scanner) scanToken() {
	c := s.advance()
	switch c {
	case ':':
		s.addToken(colon)
	case ',':
		s.addToken(comma)
	case '+':
		s.addToken(plus)
	case '!':
		if s.match(':') {
			s.addToken(bangColon)
		} else {
			s.errors = append(s.errors, fmt.Errorf("Position %d: Unexpected '!'", s.nextByte-1))
		}
	// strings
	case '"':
		s.string()
	// keyed access
	case '[':
		s.keyedAccess()
	// Ignore whitespace chars outside of "" and [].
	case ' ', '\t', '\n', '\r':
		break
	default:
		// identifiers
		//
		// We can keep it simple and not _force_ the first character to be a
		// non-number because we don't need numbers in this language. If we need
		// to extend the language to support numbers, this has to become just
		// isAlpha() and then s.identifier() will use isIdentifierChar() in
		// its main loop.
		if isIdentifierChar(c) {
			s.identifier()
			break
		}

		// TODO: We could return a more exact error message for Unicode chars if
		// we added extra handling:
		// https://stackoverflow.com/questions/53069040/checking-a-string-contains-only-ascii-characters
		s.errors = append(s.errors, fmt.Errorf("unexpected character/byte at position %d. Please avoid Unicode.", s.nextByte-1))
	}
}

// isIdentifierChar should match Kubernetes-supported name characters.
//
// https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
//
// TODO: This may not match all characters we support for cluster IDs (it may be
// the case that cluster IDs can contain UTF-8 characters).
func isIdentifierChar(b byte) bool {
	return (b >= '0' && b <= '9') || // 0-9
		(b >= 'A' && b <= 'Z') || // A-Z
		(b >= 'a' && b <= 'z') || // a-z
		b == '-' || // hyphens are allowed according to K8s spec
		b == '_' // underscores are allowed because of Prometheus sanitization
}

func (s *scanner) string() {
	for s.peek() != '"' && !s.atEnd() {
		s.advance()
	}

	if s.atEnd() {
		s.errors = append(s.errors, fmt.Errorf("unterminated string starting at %d", s.lexemeStartByte))
		return
	}

	// Consume closing '"'
	s.advance()

	s.addToken(str)
}

func (s *scanner) keyedAccess() {
	for s.peek() != ']' && !s.atEnd() {
		s.advance()
	}

	if s.atEnd() {
		s.errors = append(s.errors, fmt.Errorf("unterminated access starting at %d", s.lexemeStartByte))
		return
	}

	// Consume closing ']'
	s.advance()
	s.addToken(keyedAccess)
}

func (s *scanner) identifier() {
	for isIdentifierChar(s.peek()) {
		s.advance()
	}

	tokenText := s.source[s.lexemeStartByte:s.nextByte]
	if _, ok := ff1ToKCFilterField[tokenText]; ok {
		s.addToken(filterField1)
	} else if _, ok := ff2ToKCFilterField[tokenText]; ok {
		s.addToken(filterField2)
	} else {
		s.addToken(identifier)
	}
}

func lexAllocationFilterV2(raw string) ([]token, error) {
	s := scanner{source: raw}
	s.scanTokens()

	if len(s.errors) > 0 {
		return s.tokens, multierror.Append(nil, s.errors...)
	}

	return s.tokens, nil
}
