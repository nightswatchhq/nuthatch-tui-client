use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use serde::Deserialize;

pub(crate) const DEFAULT_URL: &str = "http://127.0.0.1:8288";

#[derive(Debug, Default)]
pub(crate) struct Args {
    pub(crate) url: Option<String>,
    pub(crate) ssh: Option<String>,
    pub(crate) nest: Option<String>,
    pub(crate) interval: Option<Duration>,
}

/// One entry in `nests.toml`: where the nest listens, and the ssh host to reach it through when
/// that is only on the host's loopback.
#[derive(Debug, Deserialize, Clone, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NestTarget {
    pub(crate) url: Option<String>,
    pub(crate) ssh: Option<String>,
    /// Token decimals for amount columns, keyed `table.column` or by column name for every table.
    /// Nuthatch serves amounts in base units and says nothing of their scale, so this is the
    /// operator's to declare.
    #[serde(default)]
    pub(crate) decimals: BTreeMap<String, u32>,
}

pub(crate) fn config_path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))?;
    Some(base.join("nuthatch-tui").join("nests.toml"))
}

pub(crate) fn parse_nests(text: &str) -> Result<BTreeMap<String, NestTarget>> {
    Ok(toml::from_str(text)?)
}

/// Flags win over the named entry, which wins over the default listener.
pub(crate) fn resolve(args: &Args, nests: &BTreeMap<String, NestTarget>) -> Result<NestTarget> {
    let named = match &args.nest {
        Some(name) => nests.get(name).cloned().with_context(|| {
            if nests.is_empty() {
                format!("no nest called '{name}': no nests are configured")
            } else {
                let known = nests.keys().cloned().collect::<Vec<_>>().join(", ");
                format!("no nest called '{name}'; configured: {known}")
            }
        })?,
        None => NestTarget::default(),
    };
    Ok(NestTarget {
        url: Some(normalize_url(
            args.url
                .clone()
                .or(named.url)
                .unwrap_or_else(|| DEFAULT_URL.into()),
        )),
        ssh: args.ssh.clone().or(named.ssh),
        decimals: named.decimals,
    })
}

const USAGE: &str = "\
nuthatch-tui-client [--url URL] [--ssh HOST] [--nest NAME] [--interval 5s]

  --url URL       the nest's API, as seen from where it runs (default http://127.0.0.1:8288)
  --ssh HOST      reach it through an ssh forward to HOST, for a nest bound to loopback there
  --nest NAME     take url and ssh from NAME in ~/.config/nuthatch-tui/nests.toml
  --interval DUR  poll this often instead of as often as the nest polls";

pub(crate) fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args> {
    let mut parsed = Args::default();
    while let Some(arg) = args.next() {
        let mut value = |what: &str| args.next().with_context(|| format!("{arg} needs {what}"));
        match arg.as_str() {
            "--url" => parsed.url = Some(normalize_url(value("a Nuthatch base URL")?)),
            "--ssh" => parsed.ssh = Some(value("an ssh host")?),
            "--nest" => parsed.nest = Some(value("a name from nests.toml")?),
            "--interval" => parsed.interval = Some(parse_interval(&value("a duration, e.g. 5s")?)?),
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument '{other}'; try --help"),
        }
    }
    Ok(parsed)
}

fn parse_interval(value: &str) -> Result<Duration> {
    let (digits, scale) = match value.strip_suffix('m') {
        Some(minutes) => (minutes, 60),
        None => (value.strip_suffix('s').unwrap_or(value), 1),
    };
    let amount: u64 = digits
        .parse()
        .with_context(|| format!("--interval '{value}' is not a duration like 5s or 2m"))?;
    anyhow::ensure!(amount > 0, "--interval must be longer than zero");
    Ok(Duration::from_secs(amount * scale))
}

pub(crate) fn normalize_url(value: String) -> String {
    value.trim_end_matches('/').to_owned()
}
