$ErrorActionPreference = "Stop"
$base = $env:CELLHIVE_SKILL_BASE_URL
if (-not $base) { $base = "https://cellhive.example.com" }
$base = $base.TrimEnd('/')
$home = if ($env:CELLHIVE_AGENT -eq "codex") { if ($env:CODEX_HOME) { $env:CODEX_HOME } else { Join-Path $HOME ".codex" } } else { if ($env:AGENTS_HOME) { $env:AGENTS_HOME } else { Join-Path $HOME ".agents" } }
$skills = Join-Path $home "skills"
$stage = Join-Path $skills (".cellhive-install-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $stage | Out-Null
try {
  $catalog = Invoke-RestMethod "$base/skills/manifest.json"
  if (-not ($catalog.version -match '^\d{4}\.\d{2}\.\d{2}\.\d+$')) { throw "invalid skill catalog version" }
  foreach ($entry in $catalog.skills) {
    if ($entry.name -ne "cellhive") { throw "unexpected skill: $($entry.name)" }
    $manifest = Invoke-RestMethod "$base/skills/$($entry.manifest)"
    if ($manifest.name -ne $entry.name -or $manifest.version -ne $catalog.version -or $manifest.entrypoint -ne "SKILL.md") { throw "invalid skill manifest" }
    $target = Join-Path $stage $entry.name
    foreach ($file in $manifest.files) {
      if ($file.StartsWith('/') -or $file.Contains('..') -or $file.Contains('\')) { throw "unsafe skill path: $file" }
      $path = Join-Path $target $file
      New-Item -ItemType Directory -Force -Path (Split-Path $path) | Out-Null
      Invoke-WebRequest "$base/skills/$($entry.name)/$file" -OutFile $path
    }
  }
  New-Item -ItemType Directory -Force -Path $skills | Out-Null
  $destination = Join-Path $skills "cellhive"
  if (Test-Path $destination) { Remove-Item -Recurse -Force $destination }
  Move-Item (Join-Path $stage "cellhive") $destination
  Set-Content -Path (Join-Path $skills ".cellhive-skills-version") -Value $catalog.version
  Write-Output "Installed CellHive skill $($catalog.version) to $destination"
} finally {
  if (Test-Path $stage) { Remove-Item -Recurse -Force $stage }
}
