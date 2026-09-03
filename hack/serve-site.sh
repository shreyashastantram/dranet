#!/usr/bin/env bash

# Copyright The Kubernetes Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Clean up any existing container
echo "Cleaning up existing container if any..."
docker stop dranet-site-dev 2>/dev/null || true
docker rm dranet-site-dev 2>/dev/null || true

echo "Starting Hugo development server..."
docker run -d \
  -v "${REPO_ROOT}/site:/src" \
  -w /src \
  -p 1313:1313 \
  --name dranet-site-dev \
  hugomods/hugo:exts server --bind 0.0.0.0

echo "Waiting for website to be ready..."
until curl -s -o /dev/null -w "%{http_code}" http://localhost:1313 | grep -q "200"; do
  sleep 1
done

echo -e "\nWebsite is UP and ready at http://localhost:1313"
echo "To view logs: docker logs -f dranet-site-dev"
echo "To stop: docker stop dranet-site-dev"
