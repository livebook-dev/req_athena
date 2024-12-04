# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.3.0](https://github.com/livebook-dev/req_athena/tree/v0.3.0) (2024-12-03)

### Added

- New querying API: `ReqAthena.new/1` + `ReqAthena.query/4` ([#43](https://github.com/livebook-dev/req_athena/pull/43))

### Removed

- **(Breaking)** Removed `ReqAthena.attach/2` in favour of `ReqAthena.new/1`([#43](https://github.com/livebook-dev/req_athena/pull/43))

## [v0.2.0](https://github.com/livebook-dev/req_athena/tree/v0.2.0) (2024-09-13)

### Changed

- This library now returns the direct result from Athena
- A `:format` option allows csv, json, or explorer data to be returned
- Improve integration with AWS Credentials

## [v0.1.5](https://github.com/livebook-dev/req_athena/tree/v0.1.5) (2023-09-01)

### Changed

- Support Req v0.4.

## [v0.1.4](https://github.com/livebook-dev/req_athena/tree/v0.1.4) (2023-08-24)

### Changed

- Update `request.options` usage to be compatible with future Req versions

## [v0.1.3](https://github.com/livebook-dev/req_athena/tree/v0.1.3) (2023-02-28)

### Fixed

- Fix reuse of `Req.Request` ([#31](https://github.com/livebook-dev/req_athena/pull/31))

## [v0.1.2](https://github.com/livebook-dev/req_athena/tree/v0.1.2) (2022-12-23)

### Changed

- Use `Enum.zip_with` instead ([#29](https://github.com/livebook-dev/req_athena/pull/29))

### Fixed

- Get query's response from a different part of `GetQueryResults` response ([#27](https://github.com/livebook-dev/req_athena/pull/27))

## [v0.1.1](https://github.com/livebook-dev/req_athena/tree/v0.1.1) (2022-07-14)

### Added

- Add support for `:aws_credentials` ([#21](https://github.com/livebook-dev/req_athena/pull/21), [#22](https://github.com/livebook-dev/req_athena/pull/22), [#23](https://github.com/livebook-dev/req_athena/pull/23), [#24](https://github.com/livebook-dev/req_athena/pull/24), [8805cfe](https://github.com/livebook-dev/req_athena/commit/8805cfebb622d56c83c3f77948dbc2ba4dae9011))

## [v0.1.0](https://github.com/livebook-dev/req_athena/tree/v0.1.0) (2022-06-29)

Initial release.
