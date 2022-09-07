#!/usr/bin/env bash
set -euo pipefail
#
# regenerate_rnds_svid: just remake the certs in the rdns & svid dirs.
#
# We're getting the hard requirements down in scripts, can integrate all into
# one all-singing all-dancing script later, so that anyone can regenerate
# without having to read test source code.
#

progname="$(basename "$0" .sh)"
note() { printf >&2 '%s: %s\n' "$progname" "$*"; }
warn() { note "$@"; }
die() { warn "$@"; exit 1; }

readonly CERT_SUBDIR='test/configs/certs'
readonly RDNS_SUBDIR='rdns'
readonly SVID_SUBDIR='svid'

# WARNING:
# This data is hard-coded into tests such as TestTLSClientAuthWithRDNSequence
# so do not "fix" it without editing the tests too!
readonly COMMON_SUB_COUNTRY=US
readonly COMMON_SUB_STATE=CA
readonly COMMON_SUB_LOCALITY='Los Angeles'
readonly COMMON_SUB_ORG=NATS
readonly COMMON_SUB_ORGUNIT=NATS
readonly COMMON_SUBJECT_OOU="/C=$COMMON_SUB_COUNTRY/ST=$COMMON_SUB_STATE/L=$COMMON_SUB_LOCALITY/O=$COMMON_SUB_ORG/OU=$COMMON_SUB_ORGUNIT"
readonly COMMON_SUBJECT_OUO="/C=$COMMON_SUB_COUNTRY/ST=$COMMON_SUB_STATE/L=$COMMON_SUB_LOCALITY/OU=$COMMON_SUB_ORGUNIT/O=$COMMON_SUB_ORG"
readonly RDNS_COMMON_SAN='subjectAltName=DNS:localhost,DNS:example.com,DNS:www.example.com'

readonly CA_KEYFILE=ca.key CA_CERTFILE=ca.pem CA_NAME='NATS CA' CA_SERIAL_FILE='ca.srl'
readonly RSA_SIZE=2048
readonly DIGEST_ALG=sha256
readonly CERT_DURATION=$((2 * 365))

REPO_TOP="$(git rev-parse --show-toplevel)"
CERT_ABSOLUTE_DIR="$REPO_TOP/$CERT_SUBDIR"
readonly REPO_TOP CERT_ABSOLUTE_DIR

okay=true
for cmd in openssl ; do
  if command -v "$cmd" >/dev/null 2>&1; then
    continue
  fi
  okay=false
  warn "missing command: $cmd"
done
$okay || die "missing necessary commands"

make_keyfile() {
  local keyfile="${1:?need a keyfile to create}"
  (umask 077; openssl genrsa "$RSA_SIZE" > "$keyfile")
}

ensure_keyfile() {
  local keyfile="${1:?need a keyfile to create}"
  local description="${2:?need a description}"
  if [ -f "$keyfile" ]; then
    note "reusing EXISTING $description file: $keyfile"
    return 0
  fi
  note "creating NEW $description file: $keyfile"
  make_keyfile "$keyfile"
}

o_req_newkey() { openssl req -newkey "rsa:$RSA_SIZE" -nodes "$@"; }

o_x509_casign() {
  local extfile_contents="$1"
  shift
  openssl x509 -req -days "$CERT_DURATION" \
    -CA "$CA_CERTFILE" -CAkey "$CA_KEYFILE" -CAcreateserial \
    -sha256 \
    -extfile <(printf '%s\n' "$extfile_contents") \
    "$@"
}

o_new_cafile() {
  local keyfile="$1" cacertfile="$2"
  shift 2
  openssl req -x509 -new -key "$CA_KEYFILE" -out "$CA_CERTFILE" -outform pem \
    -days "$CERT_DURATION" -subj "/CN=$CA_NAME" \
    "$@"
# We want these:
#   -addext subjectKeyIdentifier=hash \
#   -addext authorityKeyIdentifier=keyid:always,issuer \
#   -addext basicConstraints=critical,CA:true \
# but even without an extensions section, those seem to have been included anyway,
# resulting in a doubling when I created them, and Go cert parsing did not like
# the doubled X509v3 extensions data, leading to a panic.
# So, removed.
}

# ########################################################################
# RDNS

note "Working on rdns files"
cd "$CERT_ABSOLUTE_DIR/$RDNS_SUBDIR" || die "unable to chdir($CERT_ABSOLUTE_DIR/$RDNS_SUBDIR)"

note "creating: CA"
# This one is kept in-git, so we don't force-recreate.
# TBD: should we delete, as we do in the parent dir?
ensure_keyfile "$CA_KEYFILE" "rdns CA key"
o_new_cafile "$CA_KEYFILE" "$CA_CERTFILE"

make_rdns_client_pair() {
  local client_id="$1"
  local subject="$2"
  shift 2
  local prefix="client-$client_id"
  note "creating: $prefix"
  rm -fv -- "$prefix.key" "$prefix.csr" "$prefix.pem"
  # TBD: preserve the .key if it already exists?
  # For now, just using the same ultimate command as was documented in the tls_test.go comments,
  # so that we minimize moving parts to debug.  Key preservation is a future optimization.
  # The "$@" goes into the req invocation to let us specify -multivalue-rdn
  o_req_newkey -keyout "$prefix.key" -out "$prefix.csr" "$@" -subj "$subject" -addext extendedKeyUsage=clientAuth
  o_x509_casign "$RDNS_COMMON_SAN" -in "$prefix.csr" -out "$prefix.pem"
  rm -v -- "$prefix.csr"
  echo >&2
}

make_svid_rsa_pair() {
  local prefix="$1"
  local subject="$2"
  local san_addition="$3"
  shift 3
  note "creating: $prefix"
  rm -fv -- "$prefix.key" "$prefix.csr" "$prefix.pem"
  # TBD: preserve the .key if it already exists?
  # For now, just using the same ultimate command as was documented in the tls_test.go comments,
  # so that we minimize moving parts to debug.  Key preservation is a future optimization.
  o_req_newkey -keyout "$prefix.key" -out "$prefix.csr" -subj "$subject" -addext extendedKeyUsage=clientAuth
  o_x509_casign "$RDNS_COMMON_SAN${san_addition:+,}${san_addition:-}" -in "$prefix.csr" -out "$prefix.pem"
  rm -v -- "$prefix.csr"
  echo >&2
}

# KEEP DN STRINGS HERE MATCHING THOSE IN tls_test.go SO THAT IT's COPY/PASTE!

# C = US, ST = CA, L = Los Angeles, O = NATS, OU = NATS, CN = localhost, DC = foo1, DC = foo2
make_rdns_client_pair a "$COMMON_SUBJECT_OOU/CN=localhost/DC=foo1/DC=foo2"

# C = US, ST = California, L = Los Angeles, O = NATS, OU = NATS, CN = localhost
make_rdns_client_pair b "$COMMON_SUBJECT_OOU/CN=localhost"

# C = US, ST = CA, L = Los Angeles, O = NATS, OU = NATS, CN = localhost, DC = foo3, DC = foo4
make_rdns_client_pair c "$COMMON_SUBJECT_OOU/CN=localhost/DC=foo3/DC=foo4"

# C = US, ST = CA, L = Los Angeles, OU = NATS, O = NATS, CN = *.example.com, DC = example, DC = com
make_rdns_client_pair d "$COMMON_SUBJECT_OUO/CN=*.example.com/DC=example/DC=com"

# OpenSSL: -subj "/CN=John Doe/CN=123456/CN=jdoe/OU=Users/OU=Organic Units/DC=acme/DC=com"
make_rdns_client_pair e "/CN=John Doe/CN=123456/CN=jdoe/OU=Users/OU=Organic Units/DC=acme/DC=com"

# OpenSSL: -subj "/DC=org/DC=OpenSSL/DC=DEV+O=users/CN=John Doe"
# WITH -multivalue-rdn for the -req
make_rdns_client_pair f "/DC=org/DC=OpenSSL/DC=DEV+O=users/CN=John Doe" -multivalue-rdn

note "creating: server"
rm -fv -- "server.key" "server.csr" "server.pem"
o_req_newkey -keyout "server.key" -out "server.csr" -subj "/CN=localhost"
o_x509_casign "$RDNS_COMMON_SAN" -in "server.csr" -out "server.pem"
rm -v -- "server.csr"
echo >&2

rm -rfv "$CA_SERIAL_FILE"

# ########################################################################
# SVID

note "Working on svid files"
cd "$CERT_ABSOLUTE_DIR/$SVID_SUBDIR" || die "unable to chdir($CERT_ABSOLUTE_DIR/$SVID_SUBDIR)"

note "creating: CA"
ensure_keyfile "$CA_KEYFILE" "svid CA key"
o_new_cafile "$CA_KEYFILE" "$CA_CERTFILE"

make_svid_rsa_pair client-a "$COMMON_SUBJECT_OOU/CN=localhost/DC=foo1/DC=foo2" 'URI:spiffe://localhost/my-nats-service/user-a'

make_svid_rsa_pair client-b "/C=US/O=SPIRE" 'URI:spiffe://localhost/my-nats-service/user-b'

make_svid_rsa_pair server "/CN=localhost" ''

rm -rfv "$CA_SERIAL_FILE"

# FIXME: svid-user-a and svid-user-b are ECC certs, but not expiring at the
# same time as the rest and with differing requirements, so not coding that up
# now.
