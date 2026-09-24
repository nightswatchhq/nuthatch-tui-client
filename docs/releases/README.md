# Release notes

`release.yml` reads `docs/releases/<tag>.md` (for example `docs/releases/v0.2.0.md`) and uses it as
the GitHub release body. Write it alongside the `Cargo.toml` version bump, before tagging. A tag
without its notes fails the first job, before anything is built or published.

A tag with a pre-release identifier, the `-` in `v0.3.0-rc.1`, is published as a GitHub
pre-release and never takes the repository's "Latest release".
