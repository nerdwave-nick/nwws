package nwws

import (
	"errors"
	"strings"
)

// extension is one element of a Sec-WebSocket-Extensions header.
type extension struct {
	name   string
	params []extensionParam
}

// extensionParam is a parameter of an extension, with its value unquoted.
type extensionParam struct {
	name     string
	value    string
	hasValue bool
}

var errExtensionSyntax = errors.New("malformed Sec-WebSocket-Extensions header")

// parseExtensions parses Sec-WebSocket-Extensions header lines
// (RFC 6455 §9.1):
//
//	extension-list  = 1#extension
//	extension       = token *( ";" extension-param )
//	extension-param = token [ "=" ( token / quoted-string ) ]
//
// with optional whitespace between the elements. A quoted value must be a
// token once unquoted.
func parseExtensions(lines []string) ([]extension, error) {
	var exts []extension
	for _, line := range lines {
		p := extensionParser{s: line}
		for {
			p.skipSpace()
			if p.done() {
				break
			}
			if p.s[p.i] == ',' { // empty list elements are allowed
				p.i++
				continue
			}
			e, err := p.extension()
			if err != nil {
				return nil, err
			}
			exts = append(exts, e)
			p.skipSpace()
			if !p.done() && p.s[p.i] != ',' {
				return nil, errExtensionSyntax
			}
		}
	}
	return exts, nil
}

type extensionParser struct {
	s string
	i int
}

func (p *extensionParser) done() bool { return p.i == len(p.s) }

func (p *extensionParser) skipSpace() {
	for !p.done() && (p.s[p.i] == ' ' || p.s[p.i] == '\t') {
		p.i++
	}
}

// accept consumes c, after optional whitespace, if it is next.
func (p *extensionParser) accept(c byte) bool {
	p.skipSpace()
	if !p.done() && p.s[p.i] == c {
		p.i++
		return true
	}
	return false
}

func (p *extensionParser) extension() (extension, error) {
	name := p.token()
	if name == "" {
		return extension{}, errExtensionSyntax
	}
	e := extension{name: name}
	for p.accept(';') {
		p.skipSpace()
		param := extensionParam{name: p.token()}
		if param.name == "" {
			return extension{}, errExtensionSyntax
		}
		if p.accept('=') {
			p.skipSpace()
			v, ok := p.value()
			if !ok {
				return extension{}, errExtensionSyntax
			}
			param.value, param.hasValue = v, true
		}
		e.params = append(e.params, param)
	}
	return e, nil
}

// token consumes a token (RFC 9110 §5.6.2) and returns it, or "".
func (p *extensionParser) token() string {
	start := p.i
	for !p.done() && isTokenChar(p.s[p.i]) {
		p.i++
	}
	return p.s[start:p.i]
}

// value consumes a token or a quoted string that unquotes to a token.
func (p *extensionParser) value() (string, bool) {
	if p.done() || p.s[p.i] != '"' {
		v := p.token()
		return v, v != ""
	}
	p.i++
	var b strings.Builder
	for !p.done() {
		c := p.s[p.i]
		p.i++
		switch {
		case c == '"':
			v := b.String()
			return v, v != "" && strings.IndexFunc(v, func(r rune) bool { return r >= 0x80 || !isTokenChar(byte(r)) }) < 0
		case c == '\\' && !p.done():
			b.WriteByte(p.s[p.i])
			p.i++
		default:
			b.WriteByte(c)
		}
	}
	return "", false // unterminated
}

// isTokenChar reports whether c is a tchar (RFC 9110 §5.6.2).
func isTokenChar(c byte) bool {
	switch {
	case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		return true
	}
	return strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0
}
