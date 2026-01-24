#!/bin/bash
cd "/mnt/d/SFU/CMPT 473/a1/nats-server"
go test -v ./server -run "^(TestParseSize|TestParseSInt64|TestParseHostPort|TestURLsAreEqual|TestComma|TestURLRedaction|TestVersionAtLeast)$"