$issuer="NATS CA"
Get-ChildItem Cert:\CurrentUser\My | Where-Object {$_.Issuer -match $issuer} | Remove-Item
Get-ChildItem Cert:\CurrentUser\CA| Where-Object {$_.Issuer -match $issuer} | Remove-Item
Get-ChildItem Cert:\CurrentUser\AuthRoot | Where-Object {$_.Issuer -match $issuer} | Remove-Item
Get-ChildItem Cert:\CurrentUser\Root | Where-Object {$_.Issuer -match $issuer} | Remove-Item
