// allocationfilterutil provides functionality for parsing V2 of the Kubecost
// filter language for Allocation types.
//
// e.g. "filter=namespace:kubecost+controllerkind:deployment"
package allocationfilterutil

import (
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/opencost/opencost/pkg/kubecost"
)

// ParseAllocationFilter converts a string of the V2 Allocation Filter language
// into a kubecost.AllocationFilter.
//
// Example queries:
//   namespace:"kubecost"
//   label[app]:"cost-analyzer"
//   node!:"node1","node2"
//   cluster:"cluster-one"+namespace!:"kube-system"
//
// The grammar is approximately as follows:
//
// Original design doc [1] contains first grammar. This is a slight modification
// of that grammar to help guide the implementation of the parser.
//
// [1] https://docs.google.com/document/d/1HKkp2bv3mnvfQoBZlpHjfZwQ0FzDLOHKpnwV9gQ_KgU/edit?pli=1
//
// <filter> ::= <comparison> ('+' <comparison>)*
//              NOTE: Language can be extended to support ORs between
//              comparisons by adding a '|' operator in between comparisons,
//              though precedence will have to be carefully defined and it may
//              require adding support for ()-enclosed statements to deal with
//              precedence.
//              This would allow for queries like:
//                namespace:"x"|label[app]="foo"
//
// <comparison> ::= <filter-key> <filter-op> <filter-value>
//
// <filter-key> ::= <filter-field-2> <keyed-access>
//                | <filter-field-1>
//
// <filter-op> ::= ':' | '!:'
//
// <filter-value> ::= '"' <identifier> '"' (',' <filter-value>)*
//
// <filter-field-2> ::= 'label' | 'annotation'
//
// <filter-field-1> ::= 'cluster' | 'node' | 'namespace'
//                    | 'controllerName' | 'controllerKind'
//                    | 'container' | 'pod' | 'services'
//
// <keyed-access> ::= '[' <identifier> ']'
//
// <identifier> ::= --- valid K8s name or Prom-sanitized K8s name
func ParseAllocationFilter(filter string) (kubecost.AllocationFilter, error) {
	tokens, err := lexAllocationFilterV2(filter)
	if err != nil {
		return nil, fmt.Errorf("lexing filter: %s", err)
	}

	p := parser{tokens: tokens}

	parsedFilter, err := p.filter()
	if err != nil {
		return nil, fmt.Errorf("parsing filter: %s", err)
	}

	return parsedFilter, nil
}

// ============================================================================
// Parser
//
// Based on the Parser class in Chapter 6: Parsing Expressions of Crafting
// Interpreters by Robert Nystrom
// ============================================================================

// parseError produces error messages tailored to the needs of the parser
func parseError(t token, message string) error {
	if t.kind == eof {
		return fmt.Errorf("at end: %s", message)
	}

	return fmt.Errorf("at '%s': %s", t.s, message)
}

type parser struct {
	tokens  []token
	current int
}

// ----------------------------------------------------------------------------
// Parser helper methods for token handling
// ----------------------------------------------------------------------------

func (p *parser) atEnd() bool {
	return p.peek().kind == eof
}

func (p *parser) advance() token {
	if !p.atEnd() {
		p.current += 1
	}

	return p.previous()
}

func (p *parser) previous() token {
	return p.tokens[p.current-1]
}

// match return true and advances the parser by one token if the next token has
// a kind that matches one of the arguments. Otherwise, it returns false and
// DOES NOT advance the parser.
func (p *parser) match(tokenKinds ...tokenKind) bool {
	for _, kind := range tokenKinds {
		if p.check(kind) {
			p.advance()
			return true
		}
	}
	return false
}

// check returns true iff the next token matches the provided kind.
func (p *parser) check(tk tokenKind) bool {
	if p.atEnd() {
		return false
	}
	return p.peek().kind == tk
}

func (p *parser) peek() token {
	return p.tokens[p.current]
}

// consume is a "next token must be this kind" method. If the next token is of
// the correct kind, the parser is advanced and that token is returned. If it
// is not of the correct kind, a parse error is returned and the parser is NOT
// advanced.
func (p *parser) consume(tk tokenKind, message string) (token, error) {
	if p.check(tk) {
		return p.advance(), nil
	}

	return token{}, parseError(p.peek(), message)
}

// synchronize attempts to skip forward until the next '+', indicating the
// start of a new <comparison>. This lets us do best-effort reporting of
// multiple parse errors.
func (p *parser) synchronize() {
	p.advance()
	for !p.atEnd() {
		if p.previous().kind == plus {
			return
		}

		p.advance()
	}
}

// ----------------------------------------------------------------------------
// Parser grammar rules as recursive descent methods
// ----------------------------------------------------------------------------

// filter is the main method of the parser. It turns the token stream into an
// AllocationFilter, reporting parse errors that occurred along the way.
func (p *parser) filter() (kubecost.AllocationFilter, error) {
	var errs *multierror.Error

	// Currently, a filter is only a sequence of AND operations
	f := kubecost.AllocationFilterAnd{}
	comparison, err := p.comparison()
	if err != nil {
		errs = multierror.Append(errs, err)
		p.synchronize()
	} else {
		f.Filters = append(f.Filters, comparison)
	}
	for p.match(plus) {
		right, err := p.comparison()
		if err != nil {
			errs = multierror.Append(errs, err)
			p.synchronize()
		} else {
			f.Filters = append(f.Filters, right)
		}
	}

	return f, errs.ErrorOrNil()
}

func (p *parser) comparison() (kubecost.AllocationFilter, error) {
	field, key, err := p.filterKey()
	if err != nil {
		return nil, err
	}

	opToken, err := p.filterOp()
	if err != nil {
		return nil, err
	}

	var op kubecost.FilterOp

	switch field {
	case "services":
		switch opToken.kind {
		case colon:
			op = kubecost.FilterContains
		case bangColon:
			op = kubecost.FilterNotContains
		default:
			return nil, parseError(opToken, "implementation problem: unhandled op token for services filter")
		}
	default:
		switch opToken.kind {
		case colon:
			op = kubecost.FilterEquals
		case bangColon:
			op = kubecost.FilterNotEquals
		default:
			return nil, parseError(opToken, "implementation problem: unhandled op token")
		}

	}

	values, err := p.filterValues()
	if err != nil {
		return nil, err
	}

	switch opToken.kind {
	// In the != case, a sequence of filter values is ANDed
	// Example:
	// namespace!:"foo","bar" -> (and (notequals namespace foo)
	//                                (notequals namespace bar))
	case bangColon:
		baseFilter := kubecost.AllocationFilterAnd{}

		for _, v := range values {
			baseFilter.Filters = append(baseFilter.Filters, kubecost.AllocationFilterCondition{
				Field: field,
				Key:   key,
				Op:    op,
				Value: v,
			})
		}

		return baseFilter, nil
	default:
		baseFilter := kubecost.AllocationFilterOr{}

		for _, v := range values {
			baseFilter.Filters = append(baseFilter.Filters, kubecost.AllocationFilterCondition{
				Field: field,
				Key:   key,
				Op:    op,
				Value: v,
			})
		}

		return baseFilter, nil
	}

}

// filterKey parses a series of tokens that represent a "filter key", returning
// an error if a filter key cannot be constructed.
//
// Examples:
// tokens = [filterField2:label keyedAccess:app] -> FilterLabel, app, nil
// tokens = [filterField1:namespace] -> FilterNamespace, "", nil
func (p *parser) filterKey() (field kubecost.FilterField, key string, err error) {

	if p.match(filterField2) {
		rawField := p.previous().s
		mappedField, ok := ff2ToKCFilterField[rawField]
		if !ok {
			return "", "", parseError(p.previous(), "expect key-mapped filter field, like 'label' or 'annotation'")
		}

		_, err := p.consume(keyedAccess, "expect keyed access like '[app]' after a mapped field")
		if err != nil {
			return "", "", err
		}

		key = p.previous().s
		return mappedField, key, nil
	}

	_, err = p.consume(filterField1, "expect filter field")
	if err != nil {
		return "", "", err
	}

	rawField := p.previous().s
	mappedField, ok := ff1ToKCFilterField[rawField]
	if !ok {
		return "", "", parseError(p.previous(), "expect known filter field, like 'cluster' or 'namespace'")
	}

	return mappedField, "", nil
}

func (p *parser) filterOp() (token, error) {
	if p.match(bangColon, colon) {
		return p.previous(), nil
	}

	return token{}, parseError(p.peek(), "expect filter op like ':' or '!:'")
}

func (p *parser) filterValues() ([]string, error) {
	vals := []string{}

	_, err := p.consume(str, "expect string as filter value")
	if err != nil {
		return nil, err
	}
	vals = append(vals, p.previous().s)

	for p.match(comma) {
		_, err := p.consume(str, "expect string as filter value")
		if err != nil {
			return nil, err
		}

		vals = append(vals, p.previous().s)
	}

	return vals, nil
}
