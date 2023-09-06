$file=$args[0]
if (!$file) { $file="server.p12 "}
$fileLocale = $PSScriptRoot + "\" + $file
echo "Installing certificate $fileLocale"
$Pass = ConvertTo-SecureString -String 's3cr3t' -Force -AsPlainText
$User = "whatever"
$Cred = New-Object -TypeName "System.Management.Automation.PSCredential" -ArgumentList $User, $Pass
Import-PfxCertificate -FilePath $filelocale -CertStoreLocation Cert:\CurrentUser\My -Password $Cred.Password