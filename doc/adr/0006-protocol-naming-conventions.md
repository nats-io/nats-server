# 6. protocol-naming-conventions

Date: 2021-05-08

## Status

Under Review

## Context

This document describes naming conventions for these protocol components:

* message subject
* stream name
* reply to
* queue name
* durable name

## Prior Work

Currently the NATS Docs regarding [protocol convention](https://docs.nats.io/nats-protocol/nats-protocol#protocol-conventions) says this:

> Subject names, including reply subject (INBOX) names, are case-sensitive and must be non-empty alphanumeric strings with no embedded whitespace. All ascii alphanumeric characters except spaces/tabs and separators which are "." and ">" are allowed. Subject names can be optionally token-delimited using the dot character (.), e.g.:
A subject is comprised of 1 or more tokens. Tokens are separated by "." and can be any non space ascii alphanumeric character. The full wildcard token ">" is only valid as the last token and matches all tokens past that point. A token wildcard, "*" matches any token in the position it was listed. Wildcard tokens should only be used in a wildcard capacity and not part of a literal token.

> Character Encoding: Subject names should be ascii characters for maximum interoperability. Due to language constraints and performance, some clients may support UTF-8 subject names, as may the server. No guarantees of non-ASCII support are provided.

## Specification

```
period           = "."
asterisk         = "*"
gt               = ">"
dollar           = "$"
printable        = all printable ascii (33 to 126 inclusive)
word             = (printable except period, asterisk or gt)+
prefix           = (printable except period, asterisk, gt or dollar)+
message-subject  = word (period word | asterisk)* (period gt)?
reply-to         = word (period word | asterisk)* (period gt)?
reply-to         = word (period word)*
stream-name      = word
queue-name       = word
durable-name     = word
jetstream-prefix = prefix
```

## Alternate Unicode Support Specification

```
period           = "."
asterisk         = "*"
gt               = ">"
dollar           = "$"
printable        = all printable ascii (33 to 126 inclusive)
unicode          = all characters > 127 /and unicode
word             = (printable except period, asterisk or gt | unicode)+
prefix           = (printable except period, asterisk, gt or dollar | unicode)+
message-subject  = word (period word | asterisk)* (period gt)?
reply-to         = word (period word | asterisk)* (period gt)?
reply-to         = word (period word)*
stream-name      = word
queue-name       = word
durable-name     = word
jetstream-prefix = prefix
```
