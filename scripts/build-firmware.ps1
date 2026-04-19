# Build Ravens bike firmware and copy app binary to fw/ravens_bikeV1.bin (or -OutName).
# Requires: Arduino CLI (place arduino-cli.exe under repo tools\arduino-cli\) and ESP32 core + libraries (see sketch header).
$ErrorActionPreference = "Stop"
$Root = Split-Path $PSScriptRoot -Parent
$Cli  = Join-Path $Root "tools\arduino-cli\arduino-cli.exe"
if (-not (Test-Path $Cli)) {
  Write-Error "Missing $Cli — download Arduino CLI from https://github.com/arduino/arduino-cli/releases and extract here."
}
$Sketch = Join-Path $Root "ravens_bike"
$Fqbn   = "esp32:esp32:esp32:PartitionScheme=min_spiffs"
$OutName = if ($args[0]) { $args[0] } else { "ravens_bikeV1.bin" }

& $Cli compile --fqbn $Fqbn $Sketch
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$bin = Get-ChildItem "$env:LOCALAPPDATA\arduino\sketches" -Recurse -Filter "ravens_bike.ino.bin" -ErrorAction SilentlyContinue |
  Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $bin) {
  Write-Error "Could not find ravens_bike.ino.bin under $env:LOCALAPPDATA\arduino\sketches — check compile log."
}
$dest = Join-Path $Root "fw\$OutName"
Copy-Item $bin.FullName $dest -Force
$h = (Get-FileHash $dest -Algorithm SHA256).Hash.ToLower()
Write-Host "OK: $dest ($((Get-Item $dest).Length) bytes)"
Write-Host "SHA256: $h"
Write-Host "Update fw/ravens_bike/latest.json bin URL and sha256 to match."
