# Version Updater Script

This project includes a simple version updater script that follows semantic versioning based on commit messages. The script analyzes commit messages since the last release and determines the appropriate version bump (major, minor, or patch).

## How It Works

The version updater follows these rules:

### Pre-1.0 Versioning (0.x.y)

Before reaching version 1.0.0, the versioning scheme is slightly different:

1. **Minor Version Bump (0.x.y → 0.x+1.0)**
   - Triggered by commit messages containing "BREAKING CHANGE" or starting with "feat:" or "feature:"
   - For pre-1.0 releases, minor version bumps can include breaking changes

2. **Patch Version Bump (0.x.y → 0.x.y+1)**
   - Triggered by commit messages starting with "fix:" or "bugfix:"
   - Indicates backward-compatible bug fixes

### Post-1.0 Versioning (≥1.0.0)

Once version 1.0.0 is reached, the standard semantic versioning rules apply:

1. **Major Version Bump (x.y.z → x+1.0.0)**
   - Triggered by commit messages containing "BREAKING CHANGE"
   - Indicates backward-incompatible changes

2. **Minor Version Bump (x.y.z → x.y+1.0)**
   - Triggered by commit messages starting with "feat:" or "feature:"
   - Indicates new features that are backward compatible

3. **Patch Version Bump (x.y.z → x.y.z+1)**
   - Triggered by commit messages starting with "fix:" or "bugfix:"
   - Indicates backward-compatible bug fixes

## Commit Message Format

To properly trigger version bumps, follow these commit message formats:

- **Breaking Changes**: Include "BREAKING CHANGE" in the commit message
  ```
  feat: add new API endpoint

  BREAKING CHANGE: removed deprecated endpoint
  ```

- **Features**: Start commit messages with "feat:" or "feature:"
  ```
  feat: add new search functionality
  ```

- **Bug Fixes**: Start commit messages with "fix:" or "bugfix:"
  ```
  fix: resolve issue with pagination
  ```

## Running the Script

You can run the version updater using the provided script:

```bash
./scripts/update_version.sh
```

The script will:
1. Analyze commit messages since the last version tag
2. Determine the appropriate version bump
3. Update the version in Cargo.toml

Note: Git tagging is handled by the CI during the publish phase.

## Example Output

### Example 1: Feature in Pre-1.0

```
Compiling version updater...
Running version updater...
Current version: 0.1.0
Analyzing commit: feat: add new search functionality
  - Contains feature
New feature detected. Bumping minor version.
New version: 0.2.0
Updated Cargo.toml with new version: 0.2.0
Note: Git tagging will be handled by the CI during the publish phase.
Version update complete!
```

### Example 2: Breaking Change in Pre-1.0

```
Compiling version updater...
Running version updater...
Current version: 0.2.0
Analyzing commit: feat: redesign API
  - Contains feature
Analyzing commit: BREAKING CHANGE: removed deprecated endpoint
  - Contains BREAKING CHANGE
BREAKING CHANGE detected. Bumping minor version (pre-1.0).
New version: 0.3.0
Updated Cargo.toml with new version: 0.3.0
Note: Git tagging will be handled by the CI during the publish phase.
Version update complete!
```

### Example 3: Breaking Change in Post-1.0

```
Compiling version updater...
Running version updater...
Current version: 1.0.0
Analyzing commit: feat: redesign API
  - Contains feature
Analyzing commit: BREAKING CHANGE: removed deprecated endpoint
  - Contains BREAKING CHANGE
BREAKING CHANGE detected. Bumping major version.
New version: 2.0.0
Updated Cargo.toml with new version: 2.0.0
Note: Git tagging will be handled by the CI during the publish phase.
Version update complete!
```
