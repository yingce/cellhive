$ErrorActionPreference = "Stop"
$base = $env:CELLHIVE_SKILL_BASE_URL
if (-not $base) { throw "CELLHIVE_SKILL_BASE_URL is required (for example https://docs.example.com)" }
$base = $base.TrimEnd('/')
$skillHome = if ($env:CELLHIVE_AGENT -eq "codex") { if ($env:CODEX_HOME) { $env:CODEX_HOME } else { Join-Path $HOME ".codex" } } else { if ($env:AGENTS_HOME) { $env:AGENTS_HOME } else { Join-Path $HOME ".agents" } }
$skills = Join-Path $skillHome "skills"
$stage = Join-Path $skills (".cellhive-install-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $stage | Out-Null
try {
  $catalog = Invoke-RestMethod "$base/skills/manifest.json" -TimeoutSec 15
  if (-not ($catalog.version -match '^\d{4}\.\d{2}\.\d{2}\.\d+$')) { throw "invalid skill catalog version" }
  foreach ($entry in $catalog.skills) {
    if ($entry.name -ne "cellhive") { throw "unexpected skill: $($entry.name)" }
    $manifestPath = Join-Path $stage "manifest.json"
    Invoke-WebRequest "$base/skills/$($entry.manifest)" -OutFile $manifestPath -TimeoutSec 15
    $manifest = Get-Content -Raw $manifestPath | ConvertFrom-Json
    if ($manifest.name -ne $entry.name -or $manifest.version -ne $catalog.version -or $manifest.entrypoint -ne "SKILL.md") { throw "invalid skill manifest" }
    $target = Join-Path $stage $entry.name
    foreach ($file in $manifest.files) {
      if ($file.StartsWith('/') -or $file.Contains('..') -or $file.Contains('\')) { throw "unsafe skill path: $file" }
      $path = Join-Path $target $file
      New-Item -ItemType Directory -Force -Path (Split-Path $path) | Out-Null
      Invoke-WebRequest "$base/skills/$($entry.name)/$file" -OutFile $path -TimeoutSec 15
      if ((Get-Item $path).Length -gt 1MB) { throw "skill file too large: $file" }
      if ($file -ne "manifest.json" -and $manifest.sha256) {
        $want = $manifest.sha256.PSObject.Properties[$file].Value
        if ($want) {
          $got = (Get-FileHash -Algorithm SHA256 -Path $path).Hash
          if ($got -ine $want) { throw "checksum mismatch: $file" }
        }
      }
      if ($file -match '\.(md|json|ya?ml|txt)$') {
        $text = [IO.File]::ReadAllText($path)
        [IO.File]::WriteAllText($path, $text.Replace('https://cellhive.example.com', $base))
      }
    }
  }
  New-Item -ItemType Directory -Force -Path $skills | Out-Null
  $destination = Join-Path $skills "cellhive"
  $backup = Join-Path $skills (".cellhive-backup-" + [guid]::NewGuid().ToString("N"))
  if (Test-Path $destination) { Move-Item $destination $backup }
  try {
    Move-Item (Join-Path $stage "cellhive") $destination
  } catch {
    if (Test-Path $backup) { Move-Item $backup $destination }
    throw
  }
  if (Test-Path $backup) { Remove-Item -Recurse -Force $backup }
  Set-Content -Path (Join-Path $skills ".cellhive-skills-version") -Value $catalog.version -NoNewline
  Write-Output "Installed CellHive skill $($catalog.version) to $destination"
} finally {
  if (Test-Path $stage) { Remove-Item -Recurse -Force $stage }
}
