use std::{
    io::{self, Read},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};

use crate::{config::*, format::*};

/// An `ssh -N -L` forward to a nest that listens only on another host's loopback. `BatchMode`
/// because a password prompt would land in the middle of the dashboard, and
/// `ExitOnForwardFailure` so that a forward which cannot bind is an exit rather than a quiet ssh
/// session forwarding nothing.
pub(crate) struct Tunnel {
    program: String,
    host: String,
    pub(crate) forward: String,
    pub(crate) local_url: String,
    pub(crate) local_port: u16,
    pub(crate) child: Child,
    opened_at: Instant,
    pub(crate) failures: u32,
    pub(crate) retry_at: Option<Instant>,
    last_error: String,
}

impl Tunnel {
    pub(crate) fn open(
        program: &str,
        host: &str,
        nest_url: &str,
        quit: &AtomicBool,
    ) -> Result<Self> {
        let mut url =
            reqwest::Url::parse(nest_url).with_context(|| format!("'{nest_url}' is not a URL"))?;
        let remote_host = url
            .host_str()
            .with_context(|| format!("'{nest_url}' names no host"))?
            .to_owned();
        let remote_port = url
            .port_or_known_default()
            .with_context(|| format!("'{nest_url}' names no port"))?;
        let local_port = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        url.set_host(Some("127.0.0.1"))?;
        url.set_port(Some(local_port))
            .map_err(|_| anyhow::anyhow!("cannot set a port on '{nest_url}'"))?;
        let forward = format!("127.0.0.1:{local_port}:{remote_host}:{remote_port}");
        let child =
            spawn_ssh(program, host, &forward).with_context(|| format!("starting {program}"))?;
        let mut tunnel = Self {
            program: program.to_owned(),
            host: host.to_owned(),
            forward,
            local_url: normalize_url(url.to_string()),
            local_port,
            child,
            opened_at: Instant::now(),
            failures: 0,
            retry_at: None,
            last_error: String::new(),
        };
        tunnel.wait_until_listening(Duration::from_secs(15), quit)?;
        Ok(tunnel)
    }

    pub(crate) fn wait_until_listening(
        &mut self,
        limit: Duration,
        quit: &AtomicBool,
    ) -> Result<()> {
        let started = Instant::now();
        let address = ([127, 0, 0, 1], self.local_port).into();
        loop {
            anyhow::ensure!(!quit.load(Ordering::Relaxed), "interrupted");
            if let Some(status) = self.child.try_wait()? {
                let reason = self.stderr();
                anyhow::bail!("ssh to {} exited ({status}): {reason}", self.host);
            }
            if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok() {
                return Ok(());
            }
            anyhow::ensure!(
                started.elapsed() < limit,
                "ssh to {} had not opened the forward after {}",
                self.host,
                format_span(limit)
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn stderr(&mut self) -> String {
        let mut text = String::new();
        if let Some(mut stderr) = self.child.stderr.take() {
            let _ = stderr.read_to_string(&mut text);
        }
        let text = text.trim();
        if text.is_empty() {
            "no message".into()
        } else {
            text.lines().last().unwrap_or(text).to_owned()
        }
    }

    /// Called every loop: notices ssh exiting and reopens the forward with a doubling backoff.
    /// Returns what the footer should say while the forward is down.
    pub(crate) fn supervise(&mut self) -> Option<String> {
        if let Some(at) = self.retry_at {
            let now = Instant::now();
            if now < at {
                return Some(format!(
                    "ssh to {} exited: {}. Reopening in {}",
                    self.host,
                    self.last_error,
                    // A countdown rounds up, or the first second reads "in 0s".
                    format_span(Duration::from_secs((at - now).as_secs_f64().ceil() as u64))
                ));
            }
            self.retry_at = None;
            match spawn_ssh(&self.program, &self.host, &self.forward) {
                Ok(child) => {
                    self.child = child;
                    self.opened_at = now;
                }
                Err(error) => {
                    self.last_error = error.to_string();
                    self.schedule_retry();
                }
            }
            return Some(format!("ssh to {}: reopening the forward", self.host));
        }
        match self.child.try_wait() {
            Ok(None) => {
                if self.opened_at.elapsed() > TUNNEL_SETTLED {
                    self.failures = 0;
                }
                None
            }
            Ok(Some(_)) => {
                self.last_error = self.stderr();
                self.schedule_retry();
                self.supervise()
            }
            Err(error) => Some(format!("ssh to {}: {error}", self.host)),
        }
    }

    fn schedule_retry(&mut self) {
        self.retry_at = Some(Instant::now() + tunnel_backoff(self.failures));
        self.failures += 1;
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A forward that has stayed up this long is healthy, and its next failure starts the backoff over.
const TUNNEL_SETTLED: Duration = Duration::from_secs(60);

pub(crate) fn tunnel_backoff(failures: u32) -> Duration {
    Duration::from_secs((1u64 << failures.min(5)).min(30))
}

pub(crate) fn ssh_args(host: &str, forward: &str) -> Vec<String> {
    [
        "-N",
        "-o",
        "BatchMode=yes",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=2",
        "-L",
        forward,
        host,
    ]
    .map(String::from)
    .to_vec()
}

fn spawn_ssh(program: &str, host: &str, forward: &str) -> io::Result<Child> {
    Command::new(program)
        .args(ssh_args(host, forward))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
}
