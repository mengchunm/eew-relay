[CmdletBinding()]
param(
    [string]$Version = "",
    [string]$OutputRoot = ""
)

$ErrorActionPreference = "Stop"

$upstreamCommit = "478659ecdd75a38185d7275d154d78e9c2b752b4"
$upstreamVersion = "v2.3.5"
$patchVersion = "eew.1"
$repo = (git rev-parse --show-toplevel).Trim()
if (-not $repo) {
    throw "当前目录不在 Git 仓库中。"
}

$status = @(git -C $repo status --porcelain --untracked-files=normal)
if ($status.Count -gt 0) {
    throw "工作区存在未提交修改，停止生成 Bark 发布包。"
}

$repoCommit = (git -C $repo rev-parse HEAD).Trim()
if (-not $Version) {
    $Version = "$upstreamVersion-$patchVersion-" + (git -C $repo rev-parse --short=12 HEAD).Trim()
}
if ($Version -notmatch '^[0-9A-Za-z._-]+$') {
    throw "Version 只能包含字母、数字、点、下划线和连字符。"
}

if (-not $OutputRoot) {
    $OutputRoot = Join-Path $repo "output\bark-server\$Version"
}
$OutputRoot = [IO.Path]::GetFullPath($OutputRoot)
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

$buildRoot = Join-Path $repo ("output\bark-server-build\" + [Guid]::NewGuid().ToString("N"))
$sourceDir = Join-Path $buildRoot "source"
New-Item -ItemType Directory -Force -Path $buildRoot | Out-Null

& git clone --quiet --filter=blob:none https://github.com/Finb/bark-server.git $sourceDir
if ($LASTEXITCODE -ne 0) { throw "克隆 Bark 上游源码失败。" }
& git -C $sourceDir checkout --quiet --detach $upstreamCommit
if ($LASTEXITCODE -ne 0) { throw "检出 Bark 固定提交失败。" }
$actualUpstreamCommit = (git -C $sourceDir rev-parse HEAD).Trim()
if ($actualUpstreamCommit -ne $upstreamCommit) {
    throw "Bark 上游提交不匹配：$actualUpstreamCommit"
}

$patchFile = Join-Path $PSScriptRoot "bark-server-v2.3.5-eew.patch"
& git -C $sourceDir apply --check $patchFile
if ($LASTEXITCODE -ne 0) { throw "Bark 补丁预检失败。" }
& git -C $sourceDir apply $patchFile
if ($LASTEXITCODE -ne 0) { throw "应用 Bark 补丁失败。" }

$previousToolchain = $env:GOTOOLCHAIN
$previousGOOS = $env:GOOS
$previousGOARCH = $env:GOARCH
$previousCGO = $env:CGO_ENABLED
Push-Location $sourceDir
try {
    $env:GOTOOLCHAIN = "go1.25.6"
    $env:GOOS = ""
    $env:GOARCH = ""
    $env:CGO_ENABLED = ""
    & go test ./database ./apns
    if ($LASTEXITCODE -ne 0) { throw "Bark 补丁包测试失败。" }

    $env:GOOS = "linux"
    $env:GOARCH = "amd64"
    $env:CGO_ENABLED = "0"
    $binary = Join-Path $OutputRoot "bark-server"
    $ldflags = "-s -w -buildid= -X 'main.version=$upstreamVersion-$patchVersion' -X 'main.buildDate=reproducible' -X 'main.commitID=$upstreamCommit'"
    & go build -trimpath "-ldflags=$ldflags" -o $binary .
    if ($LASTEXITCODE -ne 0) { throw "Bark Linux amd64 构建失败。" }
}
finally {
    Pop-Location
    $env:GOTOOLCHAIN = $previousToolchain
    $env:GOOS = $previousGOOS
    $env:GOARCH = $previousGOARCH
    $env:CGO_ENABLED = $previousCGO
}

Copy-Item -LiteralPath (Join-Path $PSScriptRoot "Dockerfile.prebuilt") -Destination $OutputRoot -Force
Copy-Item -LiteralPath (Join-Path $sourceDir "LICENSE") -Destination $OutputRoot -Force
Copy-Item -LiteralPath $patchFile -Destination $OutputRoot -Force

$binary = Join-Path $OutputRoot "bark-server"
$hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $binary).Hash.ToLowerInvariant()
$utf8NoBOM = [Text.UTF8Encoding]::new($false)
[IO.File]::WriteAllText((Join-Path $OutputRoot "SHA256SUMS"), "$hash  bark-server`n", [Text.Encoding]::ASCII)

$image = "bark-server:$Version"
$releaseEnv = @(
    "BARK_RELEASE_COMMIT=$repoCommit",
    "BARK_UPSTREAM_COMMIT=$upstreamCommit",
    "BARK_SERVER_IMAGE=$image"
) -join "`n"
[IO.File]::WriteAllText((Join-Path $OutputRoot "release.env"), ($releaseEnv + "`n"), $utf8NoBOM)

$buildInfo = (& go version -m $binary 2>&1 | Out-String).TrimEnd()
[IO.File]::WriteAllText((Join-Path $OutputRoot "buildinfo.txt"), ($buildInfo + "`n"), $utf8NoBOM)

$manifest = [ordered]@{
    version = $Version
    repository_commit = $repoCommit
    upstream_version = $upstreamVersion
    upstream_commit = $upstreamCommit
    image = $image
    binary_sha256 = $hash
    go_toolchain = "go1.25.6"
    target = "linux/amd64"
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
}
$manifest | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $OutputRoot "manifest.json") -Encoding utf8NoBOM

Write-Output "Bark 发布包已在本机生成：$OutputRoot"
Write-Output "上游提交：$upstreamCommit"
Write-Output "镜像：$image"
Write-Output "SHA256：$hash"
