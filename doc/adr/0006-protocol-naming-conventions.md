# 6. protocol-naming-conventions

Date: 2021-06-28

## Status

Accepted

## Context

This document describes naming conventions for these protocol components:

* Subjects (including Reply Subjects)
* Stream Names
* Consumer Names
* Account Names

## Prior Work

Currently the NATS Docs regarding [protocol convention](https://docs.nats.io/nats-protocol/nats-protocol#protocol-conventions) says this:

> Subject names, including reply subject (INBOX) names, are case-sensitive and must be non-empty alphanumeric strings with no embedded whitespace. All ascii alphanumeric characters except spaces/tabs and separators which are "." and ">" are allowed. Subject names can be optionally token-delimited using the dot character (.), e.g.:
A subject is comprised of 1 or more tokens. Tokens are separated by "." and can be any non space ascii alphanumeric character. The full wildcard token ">" is only valid as the last token and matches all tokens past that point. A token wildcard, "*" matches any token in the position it was listed. Wildcard tokens should only be used in a wildcard capacity and not part of a literal token.

> Character Encoding: Subject names should be ascii characters for maximum interoperability. Due to language constraints and performance, some clients may support UTF-8 subject names, as may the server. No guarantees of non-ASCII support are provided.

## Specification

```
dot              = "."
asterisk         = "*"
lt               = "<"
gt               = ">"
dollar           = "$"
colon            = ":"
double-quote     = ["]
fwd-slash        = "/"
backslash        = "\"
pipe             = "|"
question-mark    = "?"
ampersand        = "&"
printable        = all printable ascii (33 to 126 inclusive)
term             = (printable except dot, asterisk or gt)+
prefix           = (printable except dot, asterisk, gt or dollar)+
filename-safe    = (printable except dot, asterisk, lt, gt, colon, double-quote, fwd-slash, backslash, pipe, question-mark, ampersand)

message-subject    = term (dot term | asterisk)* (dot gt)?
reply-to           = term (dot term)*
stream-name        = term
queue-name         = term
durable-name       = term
js-internal-prefix = dollar (prefix dot)+
js-user-prefix     = (prefix dot)+
account-name       = (filename-safe)+ maximum 255 characters
```
