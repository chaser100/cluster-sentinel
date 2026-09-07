# Changelog

All notable changes are documented in this file. The project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.1] - 2026-09-07

### Changed

- Run all CI and release jobs on the ephemeral `cluster-sentinel-k8s` runner, with serial Cargo/BuildKit compilation and bounded builder resources.
- Use ThinLTO for container builds to reduce compiler memory use on the shared runner.
- Skip external fork PR jobs and require release commits to belong to `main` and have successful push CI on `main`.
- Commit the unmodified `application:0.3.9` dependency under the Helm chart's `charts/application/` directory.
- Validate and package charts without downloading dependencies in CI or release jobs.
- Check packaged charts with an empty Helm configuration, observability values, an existing MCP Secret, and invalid schema input.
- Publish the chart after the Docker image and verify both image architectures and the deployed Helm repository.
- Serialize release runs, retain Pages artifacts for seven days, and fail on missing release archives when rebuilding the index.
- Increase the multi-architecture image build timeout to 90 minutes.
- Document the feature-branch release process, bundled dependency updates, and Pages tag permissions.

## [0.9.0] - 2026-09-04

### Added

- Initial public source release.
- Multi-architecture Docker image publication to `chaser420/cluster-sentinel`.
- Helm chart repository publication through GitHub Pages.
- Artifact Hub metadata.
- JSON Schema for Helm values validation and Artifact Hub configuration display.
- Kubernetes event RBAC, Prometheus alert rules, Grafana dashboard, and MCP authentication templates.

[0.9.1]: https://github.com/chaser100/cluster-sentinel/releases/tag/v0.9.1
[0.9.0]: https://github.com/chaser100/cluster-sentinel/releases/tag/v0.9.0
