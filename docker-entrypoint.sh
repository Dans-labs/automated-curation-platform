#!/bin/bash
set -e

seed_runtime_dir() {
  local source_dir="$1"
  local target_dir="$2"

  mkdir -p "$target_dir"
  if [ -z "$(ls -A "$target_dir" 2>/dev/null)" ]; then
    echo "Seeding $target_dir from $source_dir"
    cp -R "$source_dir"/. "$target_dir"/
  fi
}

seed_runtime_dir /bootstrap/acp/conf /home/akmi/acp/conf
seed_runtime_dir /bootstrap/acp/resources /home/akmi/acp/resources

# Create .secrets.toml from example if it doesn't exist
if [ ! -f /home/akmi/acp/conf/.secrets.toml ]; then
  echo "Creating .secrets.toml from .secrets.toml.example"
  cp /home/akmi/acp/conf/.secrets.toml.example /home/akmi/acp/conf/.secrets.toml
fi

# Start the application
python -m src.main
