$content = Get-Content "internal/cli/root.go" -Raw
$match = [regex]::Match($content, 'Version\s*=\s*"([^"]+)"')

if (-not $match.Success) {
    Write-Error "Failed to detect version from internal/cli/root.go"
    exit 1
}

Write-Output $match.Groups[1].Value
