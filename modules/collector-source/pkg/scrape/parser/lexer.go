package parser

import (
	"bufio"
	"io"
	"strings"
	"unicode"
)

type lexer struct {
	reader *bufio.Reader
	errors []error
}

func newLexer(r io.Reader) *lexer {
	return &lexer{
		reader: bufio.NewReader(r),
	}
}

func (l *lexer) nextChar() (r rune, isEof bool) {
	ch, _, err := l.reader.ReadRune()
	if err != nil {
		if err == io.EOF {
			return ch, true
		}

		l.errors = append(l.errors, err)
	}

	return ch, false
}

func (l *lexer) backup() {
	if err := l.reader.UnreadRune(); err != nil {
		l.errors = append(l.errors, err)
	}
}

func (l *lexer) next() token {
	for {
		ch, isEof := l.nextChar()
		if isEof {
			return token{Type: Eof}
		}

		switch ch {
		case '#':
			return token{Type: Comment, Value: l.comment()}
		case '{':
			return token{Type: OpenBracket, Value: "{"}
		case '}':
			return token{Type: CloseBracket, Value: "}"}
		case ',':
			return token{Type: Comma, Value: ","}
		case '=':
			return token{Type: Equal, Value: "="}
		case '"':
			return token{Type: String, Value: l.str()}
		default:
			if unicode.IsSpace(ch) {
				continue
			}

			if unicode.IsLetter(ch) {
				l.backup()

				// special handling for NaN and Inf without leading sign
				lit := l.literal()
				if lit == "NaN" || lit == "Inf" {
					return token{Type: Value, Value: lit}
				}

				return token{Type: Literal, Value: lit}
			}

			if unicode.IsDigit(ch) || ch == '.' || ch == '+' || ch == '-' {
				l.backup()
				return token{Type: Value, Value: l.float()}
			}
		}
	}
}

func (l *lexer) comment() string {
	var sb strings.Builder

	for {
		ch, isEof := l.nextChar()
		if isEof {
			return sb.String()
		}

		if ch == '\n' {
			return sb.String()
		}

		sb.WriteRune(ch)
	}
}

func (l *lexer) str() string {
	var sb strings.Builder

	for {
		r, isEof := l.nextChar()
		if isEof {
			return sb.String()
		}

		if r == '\\' {
			n, isEof := l.nextChar()
			if isEof {
				return sb.String()
			}
			if n == 'n' {
				sb.WriteRune('\n')
				continue
			}
			if n == '\\' {
				sb.WriteRune('\\')
				continue
			}
			if n == '"' {
				sb.WriteRune('"')
				continue
			}
		}

		if r != '"' {
			sb.WriteRune(r)
		} else {
			return sb.String()
		}
	}
}

func (l *lexer) literal() string {
	var sb strings.Builder

	for {
		r, isEof := l.nextChar()
		if isEof {
			return sb.String()
		}

		if isAlphaNumericUnderscore(r) {
			sb.WriteRune(r)
		} else {
			l.backup()
			return sb.String()
		}
	}
}

func (l *lexer) float() string {
	var sb strings.Builder

	for {
		r, isEof := l.nextChar()
		if isEof {
			return sb.String()
		}

		if isOneOf(r, "NaInf+-._eE") || unicode.IsDigit(r) {
			//if r == 'N' || r == 'a' || r == 'I' || r == 'n' || r == 'f' || r == '+' || r == '-' || r == '.' || r == 'e' || r == 'E' || r == '_' || unicode.IsDigit(r) {
			sb.WriteRune(r)
		} else {
			return sb.String()
		}
	}
}

func isOneOf(ch rune, chars string) bool {
	for _, c := range chars {
		if c == ch {
			return true
		}
	}

	return false
}

func isAlphaNumeric(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsDigit(ch)
}

func isAlphaNumericUnderscore(ch rune) bool {
	return isAlphaNumeric(ch) || ch == '_'
}
