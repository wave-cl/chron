#!/usr/bin/env bash
# Cuts release X.Y.Z: bumps the workspace version, moves the changelog's
# Unreleased section, runs the gate, commits and tags. It never pushes:
#   git push origin main && git push origin vX.Y.Z
# (main first, so the tag's workflow finds its commit on main.)
set -euo pipefail

version="${1:-}"
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "usage: scripts/release.sh X.Y.Z" >&2
    exit 2
fi
cd "$(git rev-parse --show-toplevel)"

if [[ -n "$(git status --porcelain)" ]]; then
    echo "the working tree is not clean; commit or stash first" >&2
    exit 1
fi
branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$branch" != "main" ]]; then
    echo "on branch $branch; releases are cut from main" >&2
    exit 1
fi
git fetch -q origin main
if [[ -n "$(git log --oneline HEAD..origin/main)" ]]; then
    echo "main is behind origin/main; pull first" >&2
    exit 1
fi
if git rev-parse -q --verify "refs/tags/v$version" >/dev/null; then
    echo "tag v$version already exists" >&2
    exit 1
fi
if ! awk '/^## Unreleased/{p=1; next} /^## /{p=0} p' CHANGELOG.md | grep -qE '^[-*]'; then
    echo "CHANGELOG.md has nothing under '## Unreleased'; add the release notes first" >&2
    exit 1
fi

# The workspace version and the four internal pins.
sed -i.bak -E "s/^version = \"[0-9]+\.[0-9]+\.[0-9]+\"$/version = \"$version\"/" Cargo.toml
sed -i.bak -E "/^(chron|chron-proto|chrond|chron-client) = \{/ s/version = \"[0-9]+\.[0-9]+\.[0-9]+\"/version = \"$version\"/" Cargo.toml
rm -f Cargo.toml.bak
grep -q "^version = \"$version\"$" Cargo.toml
[[ "$(grep -c "version = \"$version\"" Cargo.toml)" == 5 ]]
cargo update --workspace -q

# Unreleased becomes this version; a fresh Unreleased goes above it.
today="$(date +%Y-%m-%d)"
awk -v v="$version" -v d="$today" '
    /^## Unreleased/ && !done { print "## Unreleased"; print ""; print "Nothing yet."; print ""; print "## " v " - " d; done=1; next }
    /^Nothing yet\.$/ && done && !cleared { cleared=1; skip_blank=1; next }
    skip_blank && /^$/ { skip_blank=0; next }
    { print }
' CHANGELOG.md > CHANGELOG.md.new && mv CHANGELOG.md.new CHANGELOG.md

echo "== gate"
cargo fmt --all --check
cargo clippy -p chron-db --all-targets -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features --no-fail-fast

git add Cargo.toml Cargo.lock CHANGELOG.md
git -c core.hooksPath=/dev/null commit -q -m "Release $version"
git tag -a "v$version" -m "chron $version"
echo
echo "Release $version committed and tagged. Now push main, then the tag:"
echo "  git push origin main && git push origin v$version"
