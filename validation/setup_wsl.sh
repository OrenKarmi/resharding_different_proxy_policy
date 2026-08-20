#!/bin/bash
# Build the harness inside WSL so validation runs over Linux loopback instead of
# the WSL2 localhost relay, which aborts persistent connections at ~15s and
# degrades under reconnect churn.
set -e

SRC="$1"
DEST="$HOME/ReshardProbe"

export DOTNET_CLI_TELEMETRY_OPTOUT=1
export DOTNET_NOLOGO=1

if [ ! -x "$HOME/.dotnet/dotnet" ]; then
  echo "--- installing .NET SDK 8 ---"
  curl -fsSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh
  bash /tmp/dotnet-install.sh --channel 8.0 --install-dir "$HOME/.dotnet" --no-path
else
  echo "--- .NET SDK already present ---"
fi

export PATH="$HOME/.dotnet:$PATH"
dotnet --list-sdks

echo "--- copying sources (excluding bin/obj) ---"
mkdir -p "$DEST"
for f in ReshardProbe.csproj Program.cs Infra.cs Roles.cs; do
  # strip CRLF picked up from the Windows filesystem
  tr -d '\r' < "$SRC/$f" > "$DEST/$f"
done
ls -la "$DEST"

echo "--- building ---"
cd "$DEST"
dotnet build -c Release --nologo -v q

echo "SETUP_OK"
