# Golang self-signed certificate utility

This package makes it simple to generate a self-signed certificate.

Currently supports RSA keys of arbitrary length.

## Installation

`go get github.com/kthomas/go-self-signed-cert`

## Usage

`privateKey, certificate, err := selfsignedcert.Generate()`
