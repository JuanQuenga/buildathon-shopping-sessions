//! CSV dialect per rules.md: LF-only lines, every comma separates, a field may be
//! wrapped in exactly one pair of quotes around its whole value, any other quote is
//! malformed, wrong field count (incl. empty line) is malformed. Empty or `""` = null.

/// Unquote one raw field. `None` = malformed.
#[inline(always)]
pub fn unquote(f: &[u8]) -> Option<&[u8]> {
    let n = f.len();
    if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' {
        let inner = &f[1..n - 1];
        if memchr_quote(inner) {
            None
        } else {
            Some(inner)
        }
    } else if memchr_quote(f) {
        None
    } else {
        Some(f)
    }
}

#[inline(always)]
fn memchr_quote(s: &[u8]) -> bool {
    s.iter().any(|&b| b == b'"')
}

/// Split `line` (without its LF) into exactly `N` unquoted fields.
/// Returns false if malformed.
#[inline]
pub fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let mut start = 0usize;
    let mut k = 0usize;
    for (i, &b) in line.iter().enumerate() {
        if b == b',' {
            if k + 1 >= N {
                return false;
            }
            match unquote(&line[start..i]) {
                Some(f) => out[k] = f,
                None => return false,
            }
            k += 1;
            start = i + 1;
        }
    }
    if k + 1 != N {
        return false;
    }
    match unquote(&line[start..]) {
        Some(f) => out[k] = f,
        None => return false,
    }
    true
}

/// Iterate over data lines of a whole CSV file buffer: skips the first line
/// (header), yields each LF-terminated line without its LF, and a final
/// unterminated non-empty tail as a line.
pub fn data_lines(buf: &[u8]) -> impl Iterator<Item = &[u8]> {
    let start = match buf.iter().position(|&b| b == b'\n') {
        Some(p) => p + 1,
        None => buf.len(),
    };
    let body = &buf[start..];
    let body = body.strip_suffix(b"\n").unwrap_or(body);
    let empty = body.is_empty() && (start >= buf.len());
    let mut it = body.split(|&b| b == b'\n');
    if empty {
        // consume the single empty item so nothing is yielded
        it.next();
    }
    it
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dialect() {
        let mut f = [&b""[..]; 2];
        assert!(split(b"\"u4\",\"u1\"", &mut f));
        assert_eq!(f, [&b"u4"[..], b"u1"]);
        assert!(!split(b"u1,u5\"", &mut f));
        assert!(!split(b"u1", &mut f));
        assert!(!split(b"", &mut f));
        assert!(!split(b"a,b,c", &mut f));
        assert!(split(b"\"\",", &mut f));
        assert_eq!(f, [&b""[..], b""]);
        assert!(!split(b"\",a", &mut f));
        assert!(!split(b"\"a\"b\",c", &mut f));
        assert!(split(b" u1,u1\r", &mut f));
        assert_eq!(f, [&b" u1"[..], b"u1\r"]);
        let lines: Vec<&[u8]> = data_lines(b"h\na\n\nb").collect();
        assert_eq!(lines, vec![&b"a"[..], b"", b"b"]);
        assert_eq!(data_lines(b"h\n").count(), 0);
        assert_eq!(data_lines(b"h").count(), 0);
        assert_eq!(data_lines(b"").count(), 0);
        assert_eq!(data_lines(b"h\n\n").collect::<Vec<_>>(), vec![&b""[..]]);
    }
}
