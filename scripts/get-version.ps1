$content = Get-Content "internal/buildinfo/buildinfo.go" -Raw
$match = [regex]::Match($content, 'Version\s*=\s*"([^"]+)"')

if (-not $match.Success) {
    Write-Error "Failed to detect version from internal/buildinfo/buildinfo.go"
    exit 1
}

Write-Output $match.Groups[1].Value
