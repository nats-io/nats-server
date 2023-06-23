$issuer="Synadia Communications Inc."
Get-ChildItem Cert:\CurrentUser\My | Where-Object {$_.Issuer -match $issuer} | Remove-Item