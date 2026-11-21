//! Timestamp parsing per rules.md "Timestamps". Exact; no allocation.

pub const DAY_MS: i64 = 86_400_000;

#[inline]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline]
fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

#[inline]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
    }
}

#[inline(always)]
fn d(b: u8) -> Option<i64> {
    if b.is_ascii_digit() {
        Some((b - b'0') as i64)
    } else {
        None
    }
}

#[inline(always)]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    Some(d(s[i])? * 10 + d(s[i + 1])?)
}

/// Result of parsing a `ts` field.
/// `None`: not a valid timestamp syntax/calendar value.
/// `Some(v)`: epoch ms; `v == i64::MAX` means a valid F3 number too large for i64
/// (always outside any window since window_end <= i64::MAX).
#[inline]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    // F3: digits only.
    if s.len() != 20 && s.len() != 25 || s[0].is_ascii_digit() && s.iter().all(|b| b.is_ascii_digit()) {
        return parse_f3(s);
    }
    parse_iso(s)
}

#[inline]
pub fn parse_f3(s: &[u8]) -> Option<i64> {
    let mut v: u64 = 0;
    let mut overflow = false;
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
        if !overflow {
            match v.checked_mul(10).and_then(|x| x.checked_add((b - b'0') as u64)) {
                Some(x) => v = x,
                None => overflow = true,
            }
        }
    }
    if overflow || v > i64::MAX as u64 {
        Some(i64::MAX)
    } else {
        Some(v as i64)
    }
}

/// F1 `YYYY-MM-DDTHH:MM:SSZ` or F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`.
#[inline]
pub fn parse_iso(s: &[u8]) -> Option<i64> {
    if s.len() != 20 && s.len() != 25 {
        return None;
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = d2(s, 0)? * 100 + d2(s, 2)?;
    let month = d2(s, 5)?;
    let day = d2(s, 8)?;
    let hour = d2(s, 11)?;
    let minute = d2(s, 14)?;
    let second = d2(s, 17)?;
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let mut ms = (days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second) * 1000;
    if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        ms -= sign * (oh * 3600 + om * 60) * 1000;
    }
    Some(ms)
}

/// Window check helper: valid and `lo <= ts < hi`.
#[inline]
pub fn ts_in(s: &[u8], lo: i64, hi: i64) -> Option<i64> {
    match parse_ts(s) {
        Some(v) if v >= lo && v < hi => Some(v),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn forms() {
        let f1 = parse_ts(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse_ts(b"1791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"0001791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"2026-10-04T01:30:00+02:00"), parse_ts(b"2026-10-03T23:30:00Z"));
        assert_eq!(parse_ts(b"2026-10-03T23:30:00-00:00"), parse_ts(b"2026-10-03T23:30:00Z"));
        assert_eq!(parse_ts(b"99999999999999999999999"), Some(i64::MAX));
        assert_eq!(parse_ts(b"00000000000000000000000000000000000001"), Some(1));
        for bad in [
            &b""[..], b"2026-10-03T23:58:10.000Z", b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z",
            b"2026-10-03 23:58:10Z", b"2026-10-03T23:58:10", b"1791071890000.0", b"1.79e12", b"-5",
            b"2026-13-03T23:58:10Z", b"2026-02-30T23:58:10Z", b"2027-02-29T23:58:10Z",
            b"2026-10-03T24:58:10Z", b"2026-10-03T23:58:60Z", b"2026-10-03T23:58:10+24:00",
            b"2026-10-03T23:58:10+05:60", b"2026-10-03T23:58:10+0200", b"+1791071890000",
            b"2026-10-03T23:58:10+02:00 ", b"20261003T235810Z00000",
        ] {
            assert_eq!(parse_ts(bad), None, "{:?}", std::str::from_utf8(bad));
        }
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert!(parse_ts(b"2026-10-03T23:58:10+23:59").is_some());
        // 20-digit F3 strings are F3, not ISO.
        assert_eq!(parse_ts(b"00000001791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"0000000000001791071890000"), Some(1791071890000));
    }
}
