# PKCS12 Files

Refresh PKCS12 files when test certificates and keys (PEM files) are refreshed (e.g. expiry workflow)

- `client.p12` is a p12/pfx packaging of `client.pem` and `client-key.pem`

`openssl pkcs12 -export -inkey ./client-key.pem -in ./client.pem -out client.p12`

To add the CA, use the following:

`openssl pkcs12 -export -nokeys -in ..\ca.pem -out ca.p12`

> Note: set the PKCS12 bundle password to `s3cr3t` as required by provisioning scripts

## Cert Store Provisioning Scripts

Windows cert store supports p12/pfx bundle for certificate-with-key import.  Windows cert store tests will execute 
a Powershell script to import relevant PKCS12 bundle into the Windows store before the test. Equivalent to:

`powershell.exe -command "& '..\test\configs\certs\tlsauth\certstore\import-<client,server>-p12.ps1'"`

The `delete-cert-from-store.ps1` script deletes imported certificates from the Windows store (if present) that can
cause side-effects and impact the validity of different use tests.

> Note: Tests are configured for "current user" store context. Execute tests with appropriate Windows permissions
> (e.g. as Admin) if adding tests with "local machine" store context specified.