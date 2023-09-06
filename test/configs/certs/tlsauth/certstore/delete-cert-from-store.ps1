Get-ChildItem Cert:\CurrentUser\My | Where-Object {$_.Issuer -match "Synadia Communications Inc."} | Remove-Item
Get-ChildItem Cert:\CurrentUser\My | Where-Object {$_.Issuer -match "NATS.io Operators"} | Remove-Item
