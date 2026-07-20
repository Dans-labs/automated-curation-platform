#!/bin/bash
set -e

# Create .secrets.toml from example if it doesn't exist
if [ ! -f /home/akmi/acp/conf/.secrets.toml ]; then
  echo "Creating .secrets.toml from .secrets.toml.example"
  cp /home/akmi/acp/conf/.secrets.toml.example /home/akmi/acp/conf/.secrets.toml
fi

# Start the application
python -m src.main

