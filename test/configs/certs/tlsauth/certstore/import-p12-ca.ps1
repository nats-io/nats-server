$fileLocale = $PSScriptRoot + "\ca.p12"
$Pass = ConvertTo-SecureString -String 's3cr3t' -Force -AsPlainText
$User = "whatever"
$Cred = New-Object -TypeName "System.Management.Automation.PSCredential" -ArgumentList $User, $Pass
Import-PfxCertificate -FilePath $filelocale -CertStoreLocation Cert:\CurrentUser\My -Password $Cred.Password
#Import-PfxCertificate -FilePath $filelocale -CertStoreLocation Cert:\LocalMachine\Root -Password $Cred.Password
# TODO?  Move to trusted enterprise?  Requires some fingerprint parsing.