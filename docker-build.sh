#!/bin/bash

# Check if the platform parameter is provided
if [ -z "$1" ]; then
  echo "Error: No platform specified. Use 'amd64' or 'arm64'."
  exit 1
fi

# Validate the platform parameter
PLATFORM=$1
if [ "$PLATFORM" != "amd64" ] && [ "$PLATFORM" != "arm64" ]; then
  echo "Error: Invalid platform specified. Use 'amd64' or 'arm64'."
  exit 1
fi

# Path to the pyproject.toml file
PYPROJECT_TOML="/Users/akmi/surfdrive/WORK-2025/INFRA-DANS-LABS/automated-curation-platform/pyproject.toml"

# Extract the version from the pyproject.toml file
VERSION=$(grep -Eo 'version\s*=\s*"[0-9]+\.[0-9]+\.[0-9]+"' "$PYPROJECT_TOML" | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+')

# Check if the version was extracted successfully
if [ -z "$VERSION" ]; then
  echo "Error: Could not extract version from $PYPROJECT_TOML"
  exit 1
fi

# Build the Docker image with the extracted version and specified platform
echo "Building Docker image with version: $VERSION for platform: $PLATFORM"
docker buildx build --platform linux/$PLATFORM -t ekoindarto/acp:"$VERSION"-$PLATFORM --load .