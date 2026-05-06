#!/bin/sh
set -eu
#
# regenerate_top: just remake the certs in this top-dir
# we don't (currently) handle any sub-dirs
#

progname="$(basename "$0" .sh)"
note() { printf >&2 '%s: %s\n' "$progname" "$*"; }
warn() { note "$@"; }
die() { warn "$@"; exit 1; }

readonly COMMON_SUB_COUNTRY=US
readonly COMMON_SUB_STATE=California
readonly COMMON_SUB_ORG=Synadia
readonly COMMON_SUB_ORGUNIT=nats.io
readonly COMMON_SUBJECT="/C=$COMMON_SUB_COUNTRY/ST=$COMMON_SUB_STATE/O=$COMMON_SUB_ORG/OU=$COMMON_SUB_ORGUNIT"

readonly TEMP_CONFIG=openssl.cnf
readonly TEMP_CA_KEY_REL=ca-key.pem
readonly CA_FILE=ca.pem
CA_NAME="Certificate Authority $(date +%Y-%m-%d)"
readonly CA_NAME
readonly RSA_SIZE=2048
readonly DIGEST_ALG=sha256
readonly CERT_DURATION=$((10 * 365))

okay=true
for cmd in openssl ; do
  if command -v "$cmd" >/dev/null 2>&1; then
    continue
  fi
  okay=false
  warn "missing command: $cmd"
done
$okay || die "missing necessary commands"

delete_list=""
trap 'if test -n "$delete_list"; then rm -rfv $delete_list; fi' EXIT
add_delete() {
  delete_list="${delete_list:-}${delete_list:+ }$*"
}

#        Issuer: C = US, ST = CA, O = Synadia, OU = nats.io, CN = localhost, emailAddress = derek@nats.io

CA_DIR="$(mktemp -d)"
add_delete "$CA_DIR"
mkdir "$CA_DIR/copies"
touch "$CA_DIR/index.txt"

readonly CA_DIR
readonly CA_KEY="$CA_DIR/$TEMP_CA_KEY_REL"

COMMON_X509V3='
basicConstraints        = CA:FALSE
nsComment               = "nats.io nats-server test-suite certificate"
subjectKeyIdentifier    = hash
authorityKeyIdentifier  = keyid,issuer:always
subjectAltName          = ${ENV::SUBJECTALTNAME}
'

cat > "$TEMP_CONFIG" <<EOCONFIG
SUBJECTALTNAME          = email:copy
NSCERTTYPE              = server
NAME_CONSTRAINTS        =

[ ca ]
default_ca = CA_nats

[ CA_nats ]
certificate = $CA_FILE
dir = $CA_DIR
certs = \$dir/certs
new_certs_dir = \$dir/copies
crl_dir = \$dir/crl
database = \$dir/index.txt
private_key = \$dir/$TEMP_CA_KEY_REL
rand_serial = yes
unique_subject = no
# modern TLS is moving towards rejecting longer-lived certs, be prepared to lower this to less than a year and regenerate more often
default_days = $CERT_DURATION
default_md = $DIGEST_ALG
copy_extensions = copy
policy = policy_anything
x509_extensions = nats_x509_ext

[ policy_anything ]
countryName             = optional
stateOrProvinceName     = optional
localityName            = optional
organizationName        = optional
organizationalUnitName  = optional
commonName              = optional
emailAddress            = optional

[ req ]
default_bits            = $RSA_SIZE
default_md              = $DIGEST_ALG
utf8                    = yes
distinguished_name      = req_distinguished_name

[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment

[ v3_ca ]
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always,issuer:always
basicConstraints = CA:true
nsComment = "nats.io nats-server test-suite transient CA"

[ nats_x509_ext ]
$COMMON_X509V3

[ nats_server_nopeer ]
$COMMON_X509V3
nsCertType              = server
keyUsage                = digitalSignature, keyEncipherment
extendedKeyUsage        = serverAuth, nsSGC, msSGC

# NATS server certs are used as clients in peering (cluster, gateways, etc)
[ nats_server ]
$COMMON_X509V3
nsCertType              = server, client
keyUsage                = digitalSignature, keyEncipherment
extendedKeyUsage        = serverAuth, nsSGC, msSGC, clientAuth

[ nats_client ]
$COMMON_X509V3
nsCertType              = client
keyUsage                = digitalSignature, keyEncipherment
extendedKeyUsage        = clientAuth

[ req_distinguished_name ]
countryName                     = Country Name (2 letter code)
countryName_default             = $COMMON_SUB_COUNTRY
countryName_min                 = 2
countryName_max                 = 2
stateOrProvinceName             = State or Province Name (full name)
stateOrProvinceName_default     = $COMMON_SUB_STATE
0.organizationName              = Organization Name (eg, company)
0.organizationName_default      = $COMMON_SUB_ORG
organizationalUnitName          = Organizational Unit Name (eg, section)
organizationalUnitName_default  = $COMMON_SUB_ORGUNIT
commonName                      = Common Name (e.g. server FQDN or YOUR name)
commonName_max                  = 64
# no email address for our certs
EOCONFIG
add_delete "$TEMP_CONFIG"

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

o_req() { openssl req -config "$TEMP_CONFIG" "$@"; }

sign_csr() {
  local san="${1:?need subjectAltName}"
  shift
  env SUBJECTALTNAME="$san" openssl ca -config "$TEMP_CONFIG" -policy policy_anything -batch "$@"
}

make_keyfile "$CA_KEY"
o_req -x509 -new -key "$CA_KEY" -out "$CA_FILE" -outform PEM -days "$CERT_DURATION" -subj "$COMMON_SUBJECT/CN=$CA_NAME" -extensions v3_ca

echo
readonly CLIENT_KEY=client-key.pem
BASE=client-cert
ensure_keyfile "$CLIENT_KEY" "client key"
o_req -new -key "$CLIENT_KEY" -out "$BASE.csr" -subj "$COMMON_SUBJECT/CN=localhost"
add_delete "$BASE.csr"
sign_csr "DNS:localhost, IP:127.0.0.1, IP:::1, email:derek@nats.io" -in "$BASE.csr" -out "$BASE.pem" -extensions nats_client

echo
readonly CLIENT_ID_AUTH_KEY=client-id-auth-key.pem
BASE=client-id-auth-cert
ensure_keyfile "$CLIENT_ID_AUTH_KEY" "client id auth key"
o_req -new -key "$CLIENT_ID_AUTH_KEY" -out "$BASE.csr" -subj "$COMMON_SUBJECT/CN=localhost"
add_delete "$BASE.csr"
sign_csr "DNS:localhost, IP:127.0.0.1, IP:::1, email:derek@nats.io" -in "$BASE.csr" -out "$BASE.pem" -extensions nats_client

echo
readonly SERVER_KEY=server-key.pem
BASE=server-cert
ensure_keyfile "$SERVER_KEY" "server key"
o_req -new -key "$SERVER_KEY" -out "$BASE.csr" -subj "$COMMON_SUBJECT/CN=localhost"
add_delete "$BASE.csr"
sign_csr "DNS:localhost, IP:127.0.0.1, IP:::1" -in "$BASE.csr" -out "$BASE.pem" -extensions nats_server

echo
readonly SK_IPONLY=server-key-iponly.pem
BASE=server-iponly
ensure_keyfile "$SK_IPONLY" "server key, IP-only"
# Be careful not to put something verifiable that's not an IP into the CN field, for verifiers which check CN
o_req -new -key "$SK_IPONLY" -out "$BASE.csr" -subj "$COMMON_SUBJECT/CN=ip-only-localhost"
add_delete "$BASE.csr"
sign_csr "IP:127.0.0.1, IP:::1" -in "$BASE.csr" -out "$BASE.pem" -extensions nats_server

echo
readonly SK_NOIP=server-key-noip.pem
BASE=server-noip
ensure_keyfile "$SK_NOIP" "server key, no IPs"
o_req -new -key "$SK_NOIP" -out "$BASE.csr" -subj "$COMMON_SUBJECT/CN=localhost"
add_delete "$BASE.csr"
sign_csr "DNS:localhost" -in "$BASE.csr" -out "$BASE.pem" -extensions nats_server

for SRV in srva srvb; do
  echo
  KEY="${SRV}-key.pem"
  BASE="${SRV}-cert"
  ensure_keyfile "$KEY" "server key, variant $SRV"
  o_req -new -key "$KEY" -out "$BASE.csr" -subj "$COMMON_SUBJECT/CN=localhost"
  add_delete "$BASE.csr"
  sign_csr "DNS:localhost, IP:127.0.0.1, IP:::1" -in "$BASE.csr" -out "$BASE.pem" -extensions nats_server
done

