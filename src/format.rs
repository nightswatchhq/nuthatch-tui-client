use std::{collections::BTreeMap, time::Duration};

use serde_json::Value;

pub(crate) fn count_blocks(count: u64) -> String {
    match count {
        1 => "1 block".into(),
        count => format!("{} blocks", group_digits(count)),
    }
}

pub(crate) fn group_digits(value: u64) -> String {
    group_decimal(&value.to_string())
}

fn group_decimal(digits: &str) -> String {
    let (sign, digits) = digits
        .strip_prefix('-')
        .map_or(("", digits), |rest| ("-", rest));
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3 + 1);
    grouped.push_str(sign);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

/// Lifetime counters in the performance panel, which is laid out to the width of its longest line.
/// Past a million the exact figure has stopped being the point and starts costing the line its tail.
pub(crate) fn format_counter(value: u64) -> String {
    match value {
        value if value < 1_000_000 => group_digits(value),
        value if value < 1_000_000_000 => format!("{:.1}M", value as f64 / 1e6),
        value => format!("{:.1}B", value as f64 / 1e9),
    }
}

pub(crate) fn format_optional_count(value: Option<u64>) -> String {
    value.map_or_else(|| "unavailable".into(), format_counter)
}

pub(crate) fn format_span(span: Duration) -> String {
    match span.as_secs() {
        secs if secs < 120 => format!("{secs}s"),
        secs if secs < 7200 => format!("{}m", secs / 60),
        secs => format!("{}h", secs / 3600),
    }
}

/// `cpu_percent` carries the same `Option<Option<f64>>` distinction as `App::cpu_percent`.
pub(crate) fn format_cpu_percent(cpu_percent: Option<Option<f64>>) -> String {
    match cpu_percent {
        None => "unavailable (older Nuthatch)".into(),
        Some(None) => "warming up".into(),
        Some(Some(percent)) => format!("{percent:.1}%"),
    }
}

/// `None` = Nuthatch does not publish the histogram at all; `Some(None)` = published but no RPC
/// call has been observed yet; `Some(Some(ms))` = average round-trip in milliseconds, summed
/// across every RPC endpoint the way the rest of this client already aggregates labelled series.
pub(crate) fn rpc_latency_ms(metrics: &BTreeMap<String, f64>) -> Option<Option<f64>> {
    let sum = metrics.get("nuthatch_rpc_request_duration_seconds_sum")?;
    let count = metrics.get("nuthatch_rpc_request_duration_seconds_count")?;
    Some((*count > 0.0).then(|| sum / count * 1000.0))
}

pub(crate) fn format_rpc_latency(latency: Option<Option<f64>>) -> String {
    match latency {
        None => "unavailable".into(),
        Some(None) => "no calls yet".into(),
        Some(Some(ms)) => format!("{ms:.0} ms avg"),
    }
}

pub(crate) fn format_rate(value: f64, unit: &str) -> String {
    if value < 0.05 {
        format!("0 {unit}")
    } else if value < 10.0 {
        format!("{value:.1} {unit}")
    } else {
        format!("{} {unit}", group_digits(value.round() as u64))
    }
}

/// A zero here is a fact, not a gap: a nest that has sealed nothing yet genuinely occupies no
/// bytes, and saying `unavailable` would be the same misreport in the other direction. Absence is
/// `format_optional_bytes`'s job.
pub(crate) fn format_bytes(value: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    match value {
        value if value < KIB => format!("{value} B"),
        value if value < MIB => format!("{} KiB", value / KIB),
        value if value < GIB => format!("{:.1} MiB", value as f64 / MIB as f64),
        value => format!("{:.1} GiB", value as f64 / GIB as f64),
    }
}

pub(crate) fn format_optional_bytes(value: Option<u64>) -> String {
    value.map_or_else(|| "unavailable".into(), format_bytes)
}

fn shorten(value: &str, width: usize) -> String {
    if value.len() <= width {
        value.into()
    } else {
        format!("{}…", &value[..width.saturating_sub(1)])
    }
}

pub(crate) fn format_value(value: &Value) -> String {
    match value {
        Value::String(text) if text.starts_with("0x") && text.len() > 14 => {
            format!("{}…{}", &text[..6], &text[text.len() - 4..])
        }
        Value::String(text) if is_decimal(text) => format_decimal(text),
        Value::String(text) => shorten(text, 24),
        other => other.to_string(),
    }
}

/// A state call's `result` is the raw return data. One 32-byte word is how every uint getter
/// answers, so that shape is read as the number it is; anything else stays hex.
pub(crate) fn is_abi_word(text: &str) -> bool {
    text.strip_prefix("0x")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()))
}

pub(crate) fn word_to_decimal(word: &str) -> String {
    // Little-endian base-10 digits, grown one nibble at a time.
    let mut digits = vec![0u8];
    for nibble in word[2..].chars().filter_map(|c| c.to_digit(16)) {
        let mut carry = nibble;
        for digit in &mut digits {
            let value = u32::from(*digit) * 16 + carry;
            *digit = (value % 10) as u8;
            carry = value / 10;
        }
        while carry > 0 {
            digits.push((carry % 10) as u8);
            carry /= 10;
        }
    }
    let text: String = digits
        .iter()
        .rev()
        .map(|digit| char::from(b'0' + digit))
        .collect();
    let trimmed = text.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".into()
    } else {
        trimmed.into()
    }
}

pub(crate) fn is_decimal(text: &str) -> bool {
    let digits = text.strip_prefix('-').unwrap_or(text);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// A base-unit amount shown in whole tokens, to four decimal places, truncated. A non-zero amount
/// too small to show says so rather than reading as zero.
pub(crate) fn format_scaled(text: &str, decimals: u32) -> String {
    const PLACES: usize = 4;
    let (sign, digits) = text
        .strip_prefix('-')
        .map_or(("", text), |rest| ("-", rest));
    let decimals = decimals as usize;
    let padded = format!("{digits:0>width$}", width = decimals + 1);
    let (whole, fraction) = padded.split_at(padded.len() - decimals);
    let whole = whole.trim_start_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    let shown = fraction[..fraction.len().min(PLACES)].trim_end_matches('0');
    if whole == "0" && shown.is_empty() && digits.bytes().any(|b| b != b'0') {
        return format!("{sign}<0.{}1", "0".repeat(PLACES - 1));
    }
    let whole = format_decimal(&format!("{sign}{whole}"));
    if shown.is_empty() || whole.contains('e') {
        whole
    } else {
        format!("{whole}.{shown}")
    }
}

/// Big integers arrive as exact decimal text. Past fifteen digits the exact figure no longer fits a
/// feed line, and an unlimited approval (2^256 - 1) is seventy-eight of them.
pub(crate) fn format_decimal(text: &str) -> String {
    let (sign, digits) = text
        .strip_prefix('-')
        .map_or(("", text), |rest| ("-", rest));
    if digits.len() <= 15 {
        return group_decimal(text);
    }
    format!(
        "{sign}{}.{}e{}",
        &digits[..1],
        &digits[1..3],
        digits.len() - 1
    )
}
