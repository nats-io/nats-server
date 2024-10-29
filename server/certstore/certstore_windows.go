// Copyright 2022-2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Adapted, updated, and enhanced from CertToStore, https://github.com/google/certtostore/releases/tag/v1.0.2
// Apache License, Version 2.0, Copyright 2017 Google Inc.

package certstore

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"sync"
	"syscall"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/crypto/cryptobyte"
	"golang.org/x/crypto/cryptobyte/asn1"
	"golang.org/x/sys/windows"
)

const (
	// wincrypt.h constants
	winAcquireCached         = windows.CRYPT_ACQUIRE_CACHE_FLAG
	winAcquireSilent         = windows.CRYPT_ACQUIRE_SILENT_FLAG
	winAcquireOnlyNCryptKey  = windows.CRYPT_ACQUIRE_ONLY_NCRYPT_KEY_FLAG
	winEncodingX509ASN       = windows.X509_ASN_ENCODING
	winEncodingPKCS7         = windows.PKCS_7_ASN_ENCODING
	winCertStoreProvSystem   = windows.CERT_STORE_PROV_SYSTEM
	winCertStoreCurrentUser  = windows.CERT_SYSTEM_STORE_CURRENT_USER
	winCertStoreLocalMachine = windows.CERT_SYSTEM_STORE_LOCAL_MACHINE
	winCertStoreReadOnly     = windows.CERT_STORE_READONLY_FLAG
	winInfoIssuerFlag        = windows.CERT_INFO_ISSUER_FLAG
	winInfoSubjectFlag       = windows.CERT_INFO_SUBJECT_FLAG
	winCompareNameStrW       = windows.CERT_COMPARE_NAME_STR_W
	winCompareShift          = windows.CERT_COMPARE_SHIFT

	// Reference https://learn.microsoft.com/en-us/windows/win32/api/wincrypt/nf-wincrypt-certfindcertificateinstore
	winFindIssuerStr  = windows.CERT_FIND_ISSUER_STR_W
	winFindSubjectStr = windows.CERT_FIND_SUBJECT_STR_W
	winFindHashStr    = windows.CERT_FIND_HASH_STR

	winNcryptKeySpec = windows.CERT_NCRYPT_KEY_SPEC

	winBCryptPadPKCS1   uintptr = 0x2
	winBCryptPadPSS     uintptr = 0x8 // Modern TLS 1.2+
	winBCryptPadPSSSalt uint32  = 32  // default 20, 32 optimal for typical SHA256 hash

	winRSA1Magic = 0x31415352 // "RSA1" BCRYPT_RSAPUBLIC_MAGIC

	winECS1Magic = 0x31534345 // "ECS1" BCRYPT_ECDSA_PUBLIC_P256_MAGIC
	winECS3Magic = 0x33534345 // "ECS3" BCRYPT_ECDSA_PUBLIC_P384_MAGIC
	winECS5Magic = 0x35534345 // "ECS5" BCRYPT_ECDSA_PUBLIC_P521_MAGIC

	winECK1Magic = 0x314B4345 // "ECK1" BCRYPT_ECDH_PUBLIC_P256_MAGIC
	winECK3Magic = 0x334B4345 // "ECK3" BCRYPT_ECDH_PUBLIC_P384_MAGIC
	winECK5Magic = 0x354B4345 // "ECK5" BCRYPT_ECDH_PUBLIC_P521_MAGIC

	winCryptENotFound = windows.CRYPT_E_NOT_FOUND

	providerMSSoftware = "Microsoft Software Key Storage Provider"
)

var (
	winBCryptRSAPublicBlob = winWide("RSAPUBLICBLOB")
	winBCryptECCPublicBlob = winWide("ECCPUBLICBLOB")

	winNCryptAlgorithmGroupProperty = winWide("Algorithm Group") // NCRYPT_ALGORITHM_GROUP_PROPERTY
	winNCryptUniqueNameProperty     = winWide("Unique Name")     // NCRYPT_UNIQUE_NAME_PROPERTY
	winNCryptECCCurveNameProperty   = winWide("ECCCurveName")    // NCRYPT_ECC_CURVE_NAME_PROPERTY

	winCurveIDs = map[uint32]elliptic.Curve{
		winECS1Magic: elliptic.P256(), // BCRYPT_ECDSA_PUBLIC_P256_MAGIC
		winECS3Magic: elliptic.P384(), // BCRYPT_ECDSA_PUBLIC_P384_MAGIC
		winECS5Magic: elliptic.P521(), // BCRYPT_ECDSA_PUBLIC_P521_MAGIC
		winECK1Magic: elliptic.P256(), // BCRYPT_ECDH_PUBLIC_P256_MAGIC
		winECK3Magic: elliptic.P384(), // BCRYPT_ECDH_PUBLIC_P384_MAGIC
		winECK5Magic: elliptic.P521(), // BCRYPT_ECDH_PUBLIC_P521_MAGIC
	}

	winCurveNames = map[string]elliptic.Curve{
		"nistP256": elliptic.P256(), // BCRYPT_ECC_CURVE_NISTP256
		"nistP384": elliptic.P384(), // BCRYPT_ECC_CURVE_NISTP384
		"nistP521": elliptic.P521(), // BCRYPT_ECC_CURVE_NISTP521
	}

	winAlgIDs = map[crypto.Hash]*uint16{
		crypto.SHA1:   winWide("SHA1"),   // BCRYPT_SHA1_ALGORITHM
		crypto.SHA256: winWide("SHA256"), // BCRYPT_SHA256_ALGORITHM
		crypto.SHA384: winWide("SHA384"), // BCRYPT_SHA384_ALGORITHM
		crypto.SHA512: winWide("SHA512"), // BCRYPT_SHA512_ALGORITHM
	}

	// MY is well-known system store on Windows that holds personal certificates. Read
	// More about the CA locations here:
	// https://learn.microsoft.com/en-us/dotnet/framework/configure-apps/file-schema/wcf/certificate-of-clientcertificate-element?redirectedfrom=MSDN
	// https://superuser.com/questions/217719/what-are-the-windows-system-certificate-stores
	// https://docs.microsoft.com/en-us/windows/win32/seccrypto/certificate-stores
	// https://learn.microsoft.com/en-us/windows/win32/seccrypto/system-store-locations
	// https://stackoverflow.com/questions/63286085/which-x509-storename-refers-to-the-certificates-stored-beneath-trusted-root-cert#:~:text=4-,StoreName.,is%20%22Intermediate%20Certification%20Authorities%22.
	winMyStore             = winWide("MY")
	winIntermediateCAStore = winWide("CA")
	winRootStore           = winWide("Root")
	winAuthRootStore       = winWide("AuthRoot")

	// These DLLs must be available on all Windows hosts
	winCrypt32 = windows.NewLazySystemDLL("crypt32.dll")
	winNCrypt  = windows.NewLazySystemDLL("ncrypt.dll")

	winCertFindCertificateInStore        = winCrypt32.NewProc("CertFindCertificateInStore")
	winCertVerifyTimeValidity            = winCrypt32.NewProc("CertVerifyTimeValidity")
	winCryptAcquireCertificatePrivateKey = winCrypt32.NewProc("CryptAcquireCertificatePrivateKey")
	winNCryptExportKey                   = winNCrypt.NewProc("NCryptExportKey")
	winNCryptOpenStorageProvider         = winNCrypt.NewProc("NCryptOpenStorageProvider")
	winNCryptGetProperty                 = winNCrypt.NewProc("NCryptGetProperty")
	winNCryptSignHash                    = winNCrypt.NewProc("NCryptSignHash")

	winFnGetProperty = winGetProperty
)

func init() {
	for _, d := range []*windows.LazyDLL{
		winCrypt32, winNCrypt,
	} {
		if err := d.Load(); err != nil {
			panic(err)
		}
	}
	for _, p := range []*windows.LazyProc{
		winCertFindCertificateInStore, winCryptAcquireCertificatePrivateKey,
		winNCryptExportKey, winNCryptOpenStorageProvider,
		winNCryptGetProperty, winNCryptSignHash,
	} {
		if err := p.Find(); err != nil {
			panic(err)
		}
	}
}

type winPKCS1PaddingInfo struct {
	pszAlgID *uint16
}

type winPSSPaddingInfo struct {
	pszAlgID *uint16
	cbSalt   uint32
}

// createCACertsPool generates a CertPool from the Windows certificate store,
// adding all matching certificates from the caCertsMatch array to the pool.
// All matching certificates (vs first) are added to the pool based on a user
// request. If no certificates are found an error is returned.
func createCACertsPool(cs *winCertStore, storeType uint32, caCertsMatch []string, skipInvalid bool) (*x509.CertPool, error) {
	var errs []error
	caPool := x509.NewCertPool()
	for _, s := range caCertsMatch {
		lfs, err := cs.caCertsBySubjectMatch(s, storeType, skipInvalid)
		if err != nil {
			errs = append(errs, err)
		} else {
			for _, lf := range lfs {
				caPool.AddCert(lf)
			}
		}
	}
	// If every lookup failed return the errors.
	if len(errs) == len(caCertsMatch) {
		return nil, fmt.Errorf("unable to match any CA certificate: %v", errs)
	}
	return caPool, nil
}

// TLSConfig fulfills the same function as reading cert and key pair from
// pem files but sources the Windows certificate store instead. The
// certMatchBy and certMatch fields search the "MY" certificate location
// for the first certificate that matches the certMatch field. The
// caCertsMatch field is used to search the Trusted Root, Third Party Root,
// and Intermediate Certificate Authority locations for certificates with
// Subjects matching the provided strings. If a match is found, the
// certificate is added to the pool that is used to verify the certificate
// chain.
func TLSConfig(certStore StoreType, certMatchBy MatchByType, certMatch string, caCertsMatch []string, skipInvalid bool, config *tls.Config) error {
	var (
		leaf     *x509.Certificate
		leafCtx  *windows.CertContext
		pk       *winKey
		vOpts    = x509.VerifyOptions{}
		chains   [][]*x509.Certificate
		chain    []*x509.Certificate
		rawChain [][]byte
	)

	// By StoreType, open a store
	if certStore == windowsCurrentUser || certStore == windowsLocalMachine {
		var scope uint32
		cs, err := winOpenCertStore(providerMSSoftware)
		if err != nil || cs == nil {
			return err
		}
		if certStore == windowsCurrentUser {
			scope = winCertStoreCurrentUser
		}
		if certStore == windowsLocalMachine {
			scope = winCertStoreLocalMachine
		}

		// certByIssuer or certBySubject
		if certMatchBy == matchBySubject || certMatchBy == MATCHBYEMPTY {
			leaf, leafCtx, err = cs.certBySubject(certMatch, scope, skipInvalid)
		} else if certMatchBy == matchByIssuer {
			leaf, leafCtx, err = cs.certByIssuer(certMatch, scope, skipInvalid)
		} else if certMatchBy == matchByThumbprint {
			leaf, leafCtx, err = cs.certByThumbprint(certMatch, scope, skipInvalid)
		} else {
			return ErrBadMatchByType
		}
		if err != nil {
			// pass through error from cert search
			return err
		}
		if leaf == nil || leafCtx == nil {
			return ErrFailedCertSearch
		}
		pk, err = cs.certKey(leafCtx)
		if err != nil {
			return err
		}
		if pk == nil {
			return ErrNoPrivateKeyStoreRef
		}
		// Look for CA Certificates
		if len(caCertsMatch) != 0 {
			caPool, err := createCACertsPool(cs, scope, caCertsMatch, skipInvalid)
			if err != nil {
				return err
			}
			config.ClientCAs = caPool
		}
	} else {
		return ErrBadCertStore
	}

	// Get intermediates in the cert store for the found leaf IFF there is a full chain of trust in the store
	// otherwise just use leaf as the final chain.
	//
	// Using std lib Verify as a reliable way to get valid chains out of the win store for the leaf; however,
	// using empty options since server TLS stanza could be TLS role as server identity or client identity.
	chains, err := leaf.Verify(vOpts)
	if err != nil || len(chains) == 0 {
		chains = append(chains, []*x509.Certificate{leaf})
	}

	// We have at least one verified chain so pop the first chain and remove the self-signed CA cert (if present)
	// from the end of the chain
	chain = chains[0]
	if len(chain) > 1 {
		chain = chain[:len(chain)-1]
	}

	// For tls.Certificate.Certificate need a [][]byte from []*x509.Certificate
	// Approximate capacity for efficiency
	rawChain = make([][]byte, 0, len(chain))
	for _, link := range chain {
		rawChain = append(rawChain, link.Raw)
	}

	tlsCert := tls.Certificate{
		Certificate: rawChain,
		PrivateKey:  pk,
		Leaf:        leaf,
	}
	config.Certificates = []tls.Certificate{tlsCert}

	// note: pk is a windows pointer (not freed by Go) but needs to live the life of the server for Signing.
	// The cert context (leafCtx) windows pointer must not be freed underneath the pk so also life of the server.
	return nil
}

// winWide returns a pointer to uint16 representing the equivalent
// to a Windows LPCWSTR.
func winWide(s string) *uint16 {
	w := utf16.Encode([]rune(s))
	w = append(w, 0)
	return &w[0]
}

// winOpenProvider gets a provider handle for subsequent calls
func winOpenProvider(provider string) (uintptr, error) {
	var hProv uintptr
	pname := winWide(provider)
	// Open the provider, the last parameter is not used
	r, _, err := winNCryptOpenStorageProvider.Call(uintptr(unsafe.Pointer(&hProv)), uintptr(unsafe.Pointer(pname)), 0)
	if r == 0 {
		return hProv, nil
	}
	return hProv, fmt.Errorf("NCryptOpenStorageProvider returned %X: %v", r, err)
}

// winFindCert wraps the CertFindCertificateInStore library call. Note that any cert context passed
// into prev will be freed. If no certificate was found, nil will be returned.
func winFindCert(store windows.Handle, enc, findFlags, findType uint32, para *uint16, prev *windows.CertContext) (*windows.CertContext, error) {
	h, _, err := winCertFindCertificateInStore.Call(
		uintptr(store),
		uintptr(enc),
		uintptr(findFlags),
		uintptr(findType),
		uintptr(unsafe.Pointer(para)),
		uintptr(unsafe.Pointer(prev)),
	)
	if h == 0 {
		// Actual error, or simply not found?
		if errno, ok := err.(syscall.Errno); ok && errno == syscall.Errno(winCryptENotFound) {
			return nil, ErrFailedCertSearch
		}
		return nil, ErrFailedCertSearch
	}
	// nolint:govet
	return (*windows.CertContext)(unsafe.Pointer(h)), nil
}

// winVerifyCertValid wraps the CertVerifyTimeValidity and simply returns true if the certificate is valid
func winVerifyCertValid(timeToVerify *windows.Filetime, certInfo *windows.CertInfo) bool {
	// this function does not document returning errors / setting lasterror
	r, _, _ := winCertVerifyTimeValidity.Call(
		uintptr(unsafe.Pointer(timeToVerify)),
		uintptr(unsafe.Pointer(certInfo)),
	)
	return r == 0
}

// winCertStore is a store implementation for the Windows Certificate Store
type winCertStore struct {
	Prov     uintptr
	ProvName string
	stores   map[string]*winStoreHandle
	mu       sync.Mutex
}

// winOpenCertStore creates a winCertStore
func winOpenCertStore(provider string) (*winCertStore, error) {
	cngProv, err := winOpenProvider(provider)
	if err != nil {
		// pass through error from winOpenProvider
		return nil, err
	}

	wcs := &winCertStore{
		Prov:     cngProv,
		ProvName: provider,
		stores:   make(map[string]*winStoreHandle),
	}

	return wcs, nil
}

// winCertContextToX509 creates an x509.Certificate from a Windows cert context.
func winCertContextToX509(ctx *windows.CertContext) (*x509.Certificate, error) {
	var der []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&der))
	slice.Data = uintptr(unsafe.Pointer(ctx.EncodedCert))
	slice.Len = int(ctx.Length)
	slice.Cap = int(ctx.Length)
	return x509.ParseCertificate(der)
}

// certByIssuer matches and returns the first certificate found by passed issuer.
// CertContext pointer returned allows subsequent key operations like Sign. Caller specifies
// current user's personal certs or local machine's personal certs using storeType.
// See CERT_FIND_ISSUER_STR description at https://learn.microsoft.com/en-us/windows/win32/api/wincrypt/nf-wincrypt-certfindcertificateinstore
func (w *winCertStore) certByIssuer(issuer string, storeType uint32, skipInvalid bool) (*x509.Certificate, *windows.CertContext, error) {
	return w.certSearch(winFindIssuerStr, issuer, winMyStore, storeType, skipInvalid)
}

// certBySubject matches and returns the first certificate found by passed subject field.
// CertContext pointer returned allows subsequent key operations like Sign. Caller specifies
// current user's personal certs or local machine's personal certs using storeType.
// See CERT_FIND_SUBJECT_STR description at https://learn.microsoft.com/en-us/windows/win32/api/wincrypt/nf-wincrypt-certfindcertificateinstore
func (w *winCertStore) certBySubject(subject string, storeType uint32, skipInvalid bool) (*x509.Certificate, *windows.CertContext, error) {
	return w.certSearch(winFindSubjectStr, subject, winMyStore, storeType, skipInvalid)
}

// certByThumbprint matches and returns the first certificate found by passed SHA1 thumbprint.
// CertContext pointer returned allows subsequent key operations like Sign. Caller specifies
// current user's personal certs or local machine's personal certs using storeType.
// See CERT_FIND_SUBJECT_STR description at https://learn.microsoft.com/en-us/windows/win32/api/wincrypt/nf-wincrypt-certfindcertificateinstore
func (w *winCertStore) certByThumbprint(hash string, storeType uint32, skipInvalid bool) (*x509.Certificate, *windows.CertContext, error) {
	return w.certSearch(winFindHashStr, hash, winMyStore, storeType, skipInvalid)
}

// caCertsBySubjectMatch matches and returns all matching certificates of the subject field.
//
// The following locations are searched:
// 1) Root (Trusted Root Certification Authorities)
// 2) AuthRoot (Third-Party Root Certification Authorities)
// 3) CA (Intermediate Certification Authorities)
//
// Caller specifies current user's personal certs or local machine's personal certs using storeType.
// See CERT_FIND_SUBJECT_STR description at https://learn.microsoft.com/en-us/windows/win32/api/wincrypt/nf-wincrypt-certfindcertificateinstore
func (w *winCertStore) caCertsBySubjectMatch(subject string, storeType uint32, skipInvalid bool) ([]*x509.Certificate, error) {
	var (
		leaf            *x509.Certificate
		searchLocations = [3]*uint16{winRootStore, winAuthRootStore, winIntermediateCAStore}
		rv              []*x509.Certificate
	)
	// surprisingly, an empty string returns a result. We'll treat this as an error.
	if subject == "" {
		return nil, ErrBadCaCertMatchField
	}
	for _, sr := range searchLocations {
		var err error
		if leaf, _, err = w.certSearch(winFindSubjectStr, subject, sr, storeType, skipInvalid); err == nil {
			rv = append(rv, leaf)
		} else {
			// Ignore the failed search from a single location. Errors we catch include
			// ErrFailedX509Extract (resulting from a malformed certificate) and errors
			// around invalid attributes, unsupported algorithms, etc. These are corner
			// cases as certificates with these errors shouldn't have been allowed
			// to be added to the store in the first place.
			if err != ErrFailedCertSearch {
				return nil, err
			}
		}
	}
	// Not found anywhere
	if len(rv) == 0 {
		return nil, ErrFailedCertSearch
	}
	return rv, nil
}

// certSearch is a helper function to lookup certificates based on search type and match value.
// store is used to specify which store to perform the lookup in (system or user).
func (w *winCertStore) certSearch(searchType uint32, matchValue string, searchRoot *uint16, store uint32, skipInvalid bool) (*x509.Certificate, *windows.CertContext, error) {
	// store handle to "MY" store
	h, err := w.storeHandle(store, searchRoot)
	if err != nil {
		return nil, nil, err
	}

	var prev *windows.CertContext
	var cert *x509.Certificate

	i, err := windows.UTF16PtrFromString(matchValue)
	if err != nil {
		return nil, nil, ErrFailedCertSearch
	}

	// pass 0 as the third parameter because it is not used
	// https://msdn.microsoft.com/en-us/library/windows/desktop/aa376064(v=vs.85).aspx

	for {
		nc, err := winFindCert(h, winEncodingX509ASN|winEncodingPKCS7, 0, searchType, i, prev)
		if err != nil {
			return nil, nil, err
		}
		if nc != nil {
			// certificate found
			prev = nc

			var now *windows.Filetime
			if skipInvalid && !winVerifyCertValid(now, nc.CertInfo) {
				continue
			}

			// Extract the DER-encoded certificate from the cert context
			xc, err := winCertContextToX509(nc)
			if err == nil {
				cert = xc
				break
			} else {
				return nil, nil, ErrFailedX509Extract
			}
		} else {
			return nil, nil, ErrFailedCertSearch
		}
	}

	if cert == nil {
		return nil, nil, ErrFailedX509Extract
	}

	return cert, prev, nil
}

type winStoreHandle struct {
	handle *windows.Handle
}

func winNewStoreHandle(provider uint32, store *uint16) (*winStoreHandle, error) {
	var s winStoreHandle
	if s.handle != nil {
		return &s, nil
	}
	st, err := windows.CertOpenStore(
		winCertStoreProvSystem,
		0,
		0,
		provider|winCertStoreReadOnly,
		uintptr(unsafe.Pointer(store)))
	if err != nil {
		return nil, ErrBadCryptoStoreProvider
	}
	s.handle = &st
	return &s, nil
}

// winKey implements crypto.Signer and crypto.Decrypter for key based operations.
type winKey struct {
	handle         uintptr
	pub            crypto.PublicKey
	Container      string
	AlgorithmGroup string
}

// Public exports a public key to implement crypto.Signer
func (k winKey) Public() crypto.PublicKey {
	return k.pub
}

// Sign returns the signature of a hash to implement crypto.Signer
func (k winKey) Sign(_ io.Reader, digest []byte, opts crypto.SignerOpts) ([]byte, error) {
	switch k.AlgorithmGroup {
	case "ECDSA", "ECDH":
		return winSignECDSA(k.handle, digest)
	case "RSA":
		hf := opts.HashFunc()
		algID, ok := winAlgIDs[hf]
		if !ok {
			return nil, ErrBadRSAHashAlgorithm
		}
		switch opts.(type) {
		case *rsa.PSSOptions:
			return winSignRSAPSSPadding(k.handle, digest, algID)
		default:
			return winSignRSAPKCS1Padding(k.handle, digest, algID)
		}
	default:
		return nil, ErrBadSigningAlgorithm
	}
}

func winSignECDSA(kh uintptr, digest []byte) ([]byte, error) {
	var size uint32
	// Obtain the size of the signature
	r, _, _ := winNCryptSignHash.Call(
		kh,
		0,
		uintptr(unsafe.Pointer(&digest[0])),
		uintptr(len(digest)),
		0,
		0,
		uintptr(unsafe.Pointer(&size)),
		0)
	if r != 0 {
		return nil, ErrStoreECDSASigningError
	}

	// Obtain the signature data
	buf := make([]byte, size)
	r, _, _ = winNCryptSignHash.Call(
		kh,
		0,
		uintptr(unsafe.Pointer(&digest[0])),
		uintptr(len(digest)),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(size),
		uintptr(unsafe.Pointer(&size)),
		0)
	if r != 0 {
		return nil, ErrStoreECDSASigningError
	}
	if len(buf) != int(size) {
		return nil, ErrStoreECDSASigningError
	}

	return winPackECDSASigValue(bytes.NewReader(buf[:size]), len(digest))
}

func winPackECDSASigValue(r io.Reader, digestLength int) ([]byte, error) {
	sigR := make([]byte, digestLength)
	if _, err := io.ReadFull(r, sigR); err != nil {
		return nil, ErrStoreECDSASigningError
	}

	sigS := make([]byte, digestLength)
	if _, err := io.ReadFull(r, sigS); err != nil {
		return nil, ErrStoreECDSASigningError
	}

	var b cryptobyte.Builder
	b.AddASN1(asn1.SEQUENCE, func(b *cryptobyte.Builder) {
		b.AddASN1BigInt(new(big.Int).SetBytes(sigR))
		b.AddASN1BigInt(new(big.Int).SetBytes(sigS))
	})
	return b.Bytes()
}

func winSignRSAPKCS1Padding(kh uintptr, digest []byte, algID *uint16) ([]byte, error) {
	// PKCS#1 v1.5 padding for some TLS 1.2
	padInfo := winPKCS1PaddingInfo{pszAlgID: algID}
	var size uint32
	// Obtain the size of the signature
	r, _, _ := winNCryptSignHash.Call(
		kh,
		uintptr(unsafe.Pointer(&padInfo)),
		uintptr(unsafe.Pointer(&digest[0])),
		uintptr(len(digest)),
		0,
		0,
		uintptr(unsafe.Pointer(&size)),
		winBCryptPadPKCS1)
	if r != 0 {
		return nil, ErrStoreRSASigningError
	}

	// Obtain the signature data
	sig := make([]byte, size)
	r, _, _ = winNCryptSignHash.Call(
		kh,
		uintptr(unsafe.Pointer(&padInfo)),
		uintptr(unsafe.Pointer(&digest[0])),
		uintptr(len(digest)),
		uintptr(unsafe.Pointer(&sig[0])),
		uintptr(size),
		uintptr(unsafe.Pointer(&size)),
		winBCryptPadPKCS1)
	if r != 0 {
		return nil, ErrStoreRSASigningError
	}

	return sig[:size], nil
}

func winSignRSAPSSPadding(kh uintptr, digest []byte, algID *uint16) ([]byte, error) {
	// PSS padding for TLS 1.3 and some TLS 1.2
	padInfo := winPSSPaddingInfo{pszAlgID: algID, cbSalt: winBCryptPadPSSSalt}

	var size uint32
	// Obtain the size of the signature
	r, _, _ := winNCryptSignHash.Call(
		kh,
		uintptr(unsafe.Pointer(&padInfo)),
		uintptr(unsafe.Pointer(&digest[0])),
		uintptr(len(digest)),
		0,
		0,
		uintptr(unsafe.Pointer(&size)),
		winBCryptPadPSS)
	if r != 0 {
		return nil, ErrStoreRSASigningError
	}

	// Obtain the signature data
	sig := make([]byte, size)
	r, _, _ = winNCryptSignHash.Call(
		kh,
		uintptr(unsafe.Pointer(&padInfo)),
		uintptr(unsafe.Pointer(&digest[0])),
		uintptr(len(digest)),
		uintptr(unsafe.Pointer(&sig[0])),
		uintptr(size),
		uintptr(unsafe.Pointer(&size)),
		winBCryptPadPSS)
	if r != 0 {
		return nil, ErrStoreRSASigningError
	}

	return sig[:size], nil
}

// certKey wraps CryptAcquireCertificatePrivateKey. It obtains the CNG private
// key of a known certificate and returns a pointer to a winKey which implements
// both crypto.Signer. When a nil cert context is passed
// a nil key is intentionally returned, to model the expected behavior of a
// non-existent cert having no private key.
// https://docs.microsoft.com/en-us/windows/win32/api/wincrypt/nf-wincrypt-cryptacquirecertificateprivatekey
func (w *winCertStore) certKey(cert *windows.CertContext) (*winKey, error) {
	// Return early if a nil cert was passed.
	if cert == nil {
		return nil, nil
	}
	var (
		kh       uintptr
		spec     uint32
		mustFree int
	)
	r, _, _ := winCryptAcquireCertificatePrivateKey.Call(
		uintptr(unsafe.Pointer(cert)),
		winAcquireCached|winAcquireSilent|winAcquireOnlyNCryptKey,
		0, // Reserved, must be null.
		uintptr(unsafe.Pointer(&kh)),
		uintptr(unsafe.Pointer(&spec)),
		uintptr(unsafe.Pointer(&mustFree)),
	)
	// If the function succeeds, the return value is nonzero (TRUE).
	if r == 0 {
		return nil, ErrNoPrivateKeyStoreRef
	}
	if mustFree != 0 {
		return nil, ErrNoPrivateKeyStoreRef
	}
	if spec != winNcryptKeySpec {
		return nil, ErrNoPrivateKeyStoreRef
	}

	return winKeyMetadata(kh)
}

func winKeyMetadata(kh uintptr) (*winKey, error) {
	// uc is used to populate the unique container name attribute of the private key
	uc, err := winGetPropertyStr(kh, winNCryptUniqueNameProperty)
	if err != nil {
		// unable to determine key unique name
		return nil, ErrExtractingPrivateKeyMetadata
	}

	alg, err := winGetPropertyStr(kh, winNCryptAlgorithmGroupProperty)
	if err != nil {
		// unable to determine key algorithm
		return nil, ErrExtractingPrivateKeyMetadata
	}

	var pub crypto.PublicKey

	switch alg {
	case "ECDSA", "ECDH":
		buf, err := winExport(kh, winBCryptECCPublicBlob)
		if err != nil {
			// failed to export ECC public key
			return nil, ErrExtractingECCPublicKey
		}
		pub, err = unmarshalECC(buf, kh)
		if err != nil {
			return nil, ErrExtractingECCPublicKey
		}
	case "RSA":
		buf, err := winExport(kh, winBCryptRSAPublicBlob)
		if err != nil {
			return nil, ErrExtractingRSAPublicKey
		}
		pub, err = winUnmarshalRSA(buf)
		if err != nil {
			return nil, ErrExtractingRSAPublicKey
		}
	default:
		return nil, ErrBadPublicKeyAlgorithm
	}

	return &winKey{handle: kh, pub: pub, Container: uc, AlgorithmGroup: alg}, nil
}

func winGetProperty(kh uintptr, property *uint16) ([]byte, error) {
	var strSize uint32
	r, _, _ := winNCryptGetProperty.Call(
		kh,
		uintptr(unsafe.Pointer(property)),
		0,
		0,
		uintptr(unsafe.Pointer(&strSize)),
		0,
		0)
	if r != 0 {
		return nil, ErrExtractPropertyFromKey
	}

	buf := make([]byte, strSize)
	r, _, _ = winNCryptGetProperty.Call(
		kh,
		uintptr(unsafe.Pointer(property)),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(strSize),
		uintptr(unsafe.Pointer(&strSize)),
		0,
		0)
	if r != 0 {
		return nil, ErrExtractPropertyFromKey
	}

	return buf, nil
}

func winGetPropertyStr(kh uintptr, property *uint16) (string, error) {
	buf, err := winFnGetProperty(kh, property)
	if err != nil {
		return "", ErrExtractPropertyFromKey
	}
	uc := bytes.ReplaceAll(buf, []byte{0x00}, []byte(""))
	return string(uc), nil
}

func winExport(kh uintptr, blobType *uint16) ([]byte, error) {
	var size uint32
	// When obtaining the size of a public key, most parameters are not required
	r, _, _ := winNCryptExportKey.Call(
		kh,
		0,
		uintptr(unsafe.Pointer(blobType)),
		0,
		0,
		0,
		uintptr(unsafe.Pointer(&size)),
		0)
	if r != 0 {
		return nil, ErrExtractingPublicKey
	}

	// Place the exported key in buf now that we know the size required
	buf := make([]byte, size)
	r, _, _ = winNCryptExportKey.Call(
		kh,
		0,
		uintptr(unsafe.Pointer(blobType)),
		0,
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(size),
		uintptr(unsafe.Pointer(&size)),
		0)
	if r != 0 {
		return nil, ErrExtractingPublicKey
	}
	return buf, nil
}

func unmarshalECC(buf []byte, kh uintptr) (*ecdsa.PublicKey, error) {
	// BCRYPT_ECCKEY_BLOB from bcrypt.h
	header := struct {
		Magic uint32
		Key   uint32
	}{}

	r := bytes.NewReader(buf)
	if err := binary.Read(r, binary.LittleEndian, &header); err != nil {
		return nil, ErrExtractingECCPublicKey
	}

	curve, ok := winCurveIDs[header.Magic]
	if !ok {
		// Fix for b/185945636, where despite specifying the curve, nCrypt returns
		// an incorrect response with BCRYPT_ECDSA_PUBLIC_GENERIC_MAGIC.
		var err error
		curve, err = winCurveName(kh)
		if err != nil {
			// unsupported header magic or cannot match the curve by name
			return nil, err
		}
	}

	keyX := make([]byte, header.Key)
	if n, err := r.Read(keyX); n != int(header.Key) || err != nil {
		// failed to read key X
		return nil, ErrExtractingECCPublicKey
	}

	keyY := make([]byte, header.Key)
	if n, err := r.Read(keyY); n != int(header.Key) || err != nil {
		// failed to read key Y
		return nil, ErrExtractingECCPublicKey
	}

	pub := &ecdsa.PublicKey{
		Curve: curve,
		X:     new(big.Int).SetBytes(keyX),
		Y:     new(big.Int).SetBytes(keyY),
	}
	return pub, nil
}

// winCurveName reads the curve name property and returns the corresponding curve.
func winCurveName(kh uintptr) (elliptic.Curve, error) {
	cn, err := winGetPropertyStr(kh, winNCryptECCCurveNameProperty)
	if err != nil {
		// unable to determine the curve property name
		return nil, ErrExtractPropertyFromKey
	}
	curve, ok := winCurveNames[cn]
	if !ok {
		// unknown curve name
		return nil, ErrBadECCCurveName
	}
	return curve, nil
}

func winUnmarshalRSA(buf []byte) (*rsa.PublicKey, error) {
	// BCRYPT_RSA_BLOB from bcrypt.h
	header := struct {
		Magic         uint32
		BitLength     uint32
		PublicExpSize uint32
		ModulusSize   uint32
		UnusedPrime1  uint32
		UnusedPrime2  uint32
	}{}

	r := bytes.NewReader(buf)
	if err := binary.Read(r, binary.LittleEndian, &header); err != nil {
		return nil, ErrExtractingRSAPublicKey
	}

	if header.Magic != winRSA1Magic {
		// invalid header magic
		return nil, ErrExtractingRSAPublicKey
	}

	if header.PublicExpSize > 8 {
		// unsupported public exponent size
		return nil, ErrExtractingRSAPublicKey
	}

	exp := make([]byte, 8)
	if n, err := r.Read(exp[8-header.PublicExpSize:]); n != int(header.PublicExpSize) || err != nil {
		// failed to read public exponent
		return nil, ErrExtractingRSAPublicKey
	}

	mod := make([]byte, header.ModulusSize)
	if n, err := r.Read(mod); n != int(header.ModulusSize) || err != nil {
		// failed to read modulus
		return nil, ErrExtractingRSAPublicKey
	}

	pub := &rsa.PublicKey{
		N: new(big.Int).SetBytes(mod),
		E: int(binary.BigEndian.Uint64(exp)),
	}
	return pub, nil
}

// storeHandle returns a handle to a given cert store, opening the handle as needed.
func (w *winCertStore) storeHandle(provider uint32, store *uint16) (windows.Handle, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	key := fmt.Sprintf("%d%s", provider, windows.UTF16PtrToString(store))
	var err error
	if w.stores[key] == nil {
		w.stores[key], err = winNewStoreHandle(provider, store)
		if err != nil {
			return 0, ErrBadCryptoStoreProvider
		}
	}
	return *w.stores[key].handle, nil
}

// Verify interface conformance.
var _ credential = &winKey{}
