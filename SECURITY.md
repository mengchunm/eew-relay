# Security Policy

Please do not report Bark Keys, relay tokens, database credentials, production URLs, or other secrets in public issues.

For security-sensitive reports, contact the maintainer privately through the contact method listed on the GitHub profile. Include the affected version, reproduction steps, impact, and any suggested mitigation. Remove or mask all real user credentials and production data.

Deployments should use a dedicated Bark Server, unique relay and queue tokens, HTTPS, restricted configuration file permissions, and a current Go toolchain. Run `govulncheck ./...` before publishing a release.
