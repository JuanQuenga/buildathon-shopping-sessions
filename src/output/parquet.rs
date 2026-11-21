//! Minimal hand-rolled Parquet writer: one row group per file, one uncompressed PLAIN
//! data page (v1) per column chunk, no statistics. Supports exactly what the two
//! output tables need: required/optional UTF-8 BYTE_ARRAY, required INT64 and
//! required INT32 DATE. Footer is Thrift compact protocol written by hand.

const PAR1: &[u8; 4] = b"PAR1";

// parquet.thrift enum values
const T_INT32: i32 = 1;
const T_INT64: i32 = 2;
const T_BYTE_ARRAY: i32 = 6;
const REP_REQUIRED: i32 = 0;
const REP_OPTIONAL: i32 = 1;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;
const CONV_UTF8: i32 = 0;
const CONV_DATE: i32 = 6;
const PAGE_DATA: i32 = 0;
const CODEC_UNCOMPRESSED: i32 = 0;

// thrift compact type ids
const CT_TRUE: u8 = 1;
const CT_I32: u8 = 5;
const CT_I64: u8 = 6;
const CT_BINARY: u8 = 8;
const CT_LIST: u8 = 9;
const CT_STRUCT: u8 = 12;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Utf8,
    OptUtf8,
    Int64,
    Date,
}

struct ColMeta {
    name: &'static str,
    kind: Kind,
    /// Offset of the page header in the file.
    offset: u64,
    /// Bytes of header + page.
    size: u64,
}

/// Builds a complete Parquet file in memory. Columns must be added in schema order,
/// each with exactly `num_rows` values.
pub struct FileBuilder {
    buf: Vec<u8>,
    num_rows: usize,
    cols: Vec<ColMeta>,
}

#[inline]
fn uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[inline]
fn zz(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// Thrift compact protocol writer (only what the footer and page headers need).
struct Tw<'a> {
    out: &'a mut Vec<u8>,
    last: Vec<i16>,
    cur: i16,
}

impl<'a> Tw<'a> {
    fn new(out: &'a mut Vec<u8>) -> Self {
        Tw {
            out,
            last: Vec::new(),
            cur: 0,
        }
    }
    fn field(&mut self, id: i16, ty: u8) {
        let delta = id - self.cur;
        if delta > 0 && delta <= 15 {
            self.out.push(((delta as u8) << 4) | ty);
        } else {
            self.out.push(ty);
            uvarint(self.out, zz(id as i64));
        }
        self.cur = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, CT_I32);
        uvarint(self.out, zz(v as i64));
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, CT_I64);
        uvarint(self.out, zz(v));
    }
    fn binary(&mut self, id: i16, v: &[u8]) {
        self.field(id, CT_BINARY);
        uvarint(self.out, v.len() as u64);
        self.out.extend_from_slice(v);
    }
    #[allow(dead_code)]
    fn bool_true(&mut self, id: i16) {
        self.field(id, CT_TRUE);
    }
    /// Begin a nested struct (as field `id`, or as a list element when `id` is None).
    fn begin(&mut self, id: Option<i16>) {
        if let Some(id) = id {
            self.field(id, CT_STRUCT);
        }
        self.last.push(self.cur);
        self.cur = 0;
    }
    fn end(&mut self) {
        self.out.push(0);
        self.cur = self.last.pop().unwrap_or(0);
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, CT_LIST);
        if n < 15 {
            self.out.push(((n as u8) << 4) | elem);
        } else {
            self.out.push(0xF0 | elem);
            uvarint(self.out, n as u64);
        }
    }
    fn raw_i32(&mut self, v: i32) {
        uvarint(self.out, zz(v as i64));
    }
    fn raw_binary(&mut self, v: &[u8]) {
        uvarint(self.out, v.len() as u64);
        self.out.extend_from_slice(v);
    }
}

fn varint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

impl FileBuilder {
    pub fn new(num_rows: usize, capacity: usize) -> Self {
        let mut buf = Vec::with_capacity(capacity + 4096);
        buf.extend_from_slice(PAR1);
        FileBuilder {
            buf,
            num_rows,
            cols: Vec::new(),
        }
    }

    fn begin_col(&mut self, name: &'static str, kind: Kind, page_size: usize) -> u64 {
        assert!(page_size <= i32::MAX as usize, "page too large");
        let offset = self.buf.len() as u64;
        page_header(&mut self.buf, page_size, self.num_rows);
        self.buf.reserve(page_size);
        self.cols.push(ColMeta {
            name,
            kind,
            offset,
            size: 0,
        });
        offset
    }

    fn end_col(&mut self) {
        let len = self.buf.len() as u64;
        let c = self.cols.last_mut().unwrap();
        c.size = len - c.offset;
    }

    pub fn col_i64(&mut self, name: &'static str, vals: impl ExactSizeIterator<Item = i64>) {
        assert_eq!(vals.len(), self.num_rows);
        self.begin_col(name, Kind::Int64, 8 * self.num_rows);
        for v in vals {
            self.buf.extend_from_slice(&v.to_le_bytes());
        }
        self.end_col();
    }

    pub fn col_date(&mut self, name: &'static str, vals: impl ExactSizeIterator<Item = i32>) {
        assert_eq!(vals.len(), self.num_rows);
        self.begin_col(name, Kind::Date, 4 * self.num_rows);
        for v in vals {
            self.buf.extend_from_slice(&v.to_le_bytes());
        }
        self.end_col();
    }

    /// Required UTF-8 column. `total_bytes` must be the sum of value lengths.
    pub fn col_utf8<'b>(
        &mut self,
        name: &'static str,
        total_bytes: usize,
        vals: impl ExactSizeIterator<Item = &'b [u8]>,
    ) {
        assert_eq!(vals.len(), self.num_rows);
        let size = 4 * self.num_rows + total_bytes;
        self.begin_col(name, Kind::Utf8, size);
        let start = self.buf.len();
        for v in vals {
            self.buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            self.buf.extend_from_slice(v);
        }
        assert_eq!(self.buf.len() - start, size, "utf8 size mismatch");
        self.end_col();
    }

    /// Optional UTF-8 column. `nonnull` = number of Some values, `total_bytes` = their total length.
    pub fn col_opt_utf8<'b>(
        &mut self,
        name: &'static str,
        nonnull: usize,
        total_bytes: usize,
        vals: impl ExactSizeIterator<Item = Option<&'b [u8]>> + Clone,
    ) {
        let n = self.num_rows;
        assert_eq!(vals.len(), n);
        // Definition levels: RLE/bit-packed hybrid, one bit-packed run, 4-byte length prefix.
        let groups = n.div_ceil(8);
        let hdr = ((groups as u64) << 1) | 1;
        let lvl_len = if n == 0 { 0 } else { varint_len(hdr) + groups };
        let size = 4 + lvl_len + 4 * nonnull + total_bytes;
        self.begin_col(name, Kind::OptUtf8, size);
        let start = self.buf.len();
        self.buf.extend_from_slice(&(lvl_len as u32).to_le_bytes());
        if n > 0 {
            uvarint(&mut self.buf, hdr);
            let mut byte = 0u8;
            let mut bit = 0;
            for v in vals.clone() {
                byte |= (v.is_some() as u8) << bit;
                bit += 1;
                if bit == 8 {
                    self.buf.push(byte);
                    byte = 0;
                    bit = 0;
                }
            }
            if bit != 0 {
                self.buf.push(byte);
            }
        }
        for v in vals.flatten() {
            self.buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            self.buf.extend_from_slice(v);
        }
        assert_eq!(self.buf.len() - start, size, "opt utf8 size mismatch");
        self.end_col();
    }

    /// Writes the footer and returns the complete file bytes.
    pub fn finish(mut self) -> Vec<u8> {
        let cols = std::mem::take(&mut self.cols);
        footer(&mut self.buf, self.num_rows, &cols);
        self.buf
    }
}

fn page_header(out: &mut Vec<u8>, page_size: usize, num_values: usize) {
    assert!(page_size <= i32::MAX as usize, "parquet page too large");
    let mut tw = Tw::new(out);
    tw.i32(1, PAGE_DATA);
    tw.i32(2, page_size as i32);
    tw.i32(3, page_size as i32);
    tw.begin(Some(5));
    tw.i32(1, num_values as i32);
    tw.i32(2, ENC_PLAIN);
    tw.i32(3, ENC_RLE);
    tw.i32(4, ENC_RLE);
    tw.end();
    tw.out.push(0); // end PageHeader
}

/// Appends FileMetaData, its length and the trailing magic.
fn footer(out: &mut Vec<u8>, num_rows: usize, cols: &[ColMeta]) {
    let footer_start = out.len();
    let num_rows = num_rows as i64;
    {
        let mut tw = Tw::new(out);
        tw.i32(1, 1); // version
        tw.list(2, CT_STRUCT, cols.len() + 1);
        // root
        tw.begin(None);
        tw.binary(4, b"schema");
        tw.i32(5, cols.len() as i32);
        tw.end();
        for c in cols.iter() {
            tw.begin(None);
            let (ty, rep) = match c.kind {
                Kind::Utf8 => (T_BYTE_ARRAY, REP_REQUIRED),
                Kind::OptUtf8 => (T_BYTE_ARRAY, REP_OPTIONAL),
                Kind::Int64 => (T_INT64, REP_REQUIRED),
                Kind::Date => (T_INT32, REP_REQUIRED),
            };
            tw.i32(1, ty);
            tw.i32(3, rep);
            tw.binary(4, c.name.as_bytes());
            match c.kind {
                Kind::Utf8 | Kind::OptUtf8 => {
                    tw.i32(6, CONV_UTF8);
                    tw.begin(Some(10)); // LogicalType
                    tw.begin(Some(1)); // STRING
                    tw.end();
                    tw.end();
                }
                Kind::Date => {
                    tw.i32(6, CONV_DATE);
                    tw.begin(Some(10));
                    tw.begin(Some(6)); // DATE
                    tw.end();
                    tw.end();
                }
                Kind::Int64 => {}
            }
            tw.end();
        }
        tw.i64(3, num_rows);
        if num_rows == 0 {
            tw.list(4, CT_STRUCT, 0);
        } else {
            tw.list(4, CT_STRUCT, 1);
            tw.begin(None); // RowGroup
            tw.list(1, CT_STRUCT, cols.len());
            let mut total = 0i64;
            for c in cols.iter() {
                total += c.size as i64;
                tw.begin(None); // ColumnChunk
                tw.i64(2, c.offset as i64);
                tw.begin(Some(3)); // ColumnMetaData
                let ty = match c.kind {
                    Kind::Utf8 | Kind::OptUtf8 => T_BYTE_ARRAY,
                    Kind::Int64 => T_INT64,
                    Kind::Date => T_INT32,
                };
                tw.i32(1, ty);
                tw.list(2, CT_I32, 2);
                tw.raw_i32(ENC_PLAIN);
                tw.raw_i32(ENC_RLE);
                tw.list(3, CT_BINARY, 1);
                tw.raw_binary(c.name.as_bytes());
                tw.i32(4, CODEC_UNCOMPRESSED);
                tw.i64(5, num_rows);
                tw.i64(6, c.size as i64);
                tw.i64(7, c.size as i64);
                tw.i64(9, c.offset as i64);
                tw.end(); // ColumnMetaData
                tw.end(); // ColumnChunk
            }
            tw.i64(2, total);
            tw.i64(3, num_rows);
            tw.i64(5, cols[0].offset as i64);
            tw.i64(6, total);
            tw.end(); // RowGroup
        }
        tw.binary(6, b"shopping-sessions native writer");
        tw.out.push(0); // end FileMetaData
    }
    let footer_len = (out.len() - footer_start) as u32;
    out.extend_from_slice(&footer_len.to_le_bytes());
    out.extend_from_slice(PAR1);
}

/// One column for `write_columns`: the PLAIN page body given as parts, written as-is.
/// For `Kind::OptUtf8` the parts must start with the definition levels (use `def_levels_prefix`).
pub struct Column<'a> {
    pub name: &'static str,
    pub kind: Kind,
    pub parts: Vec<&'a [u8]>,
}

/// Prefix for an optional column's page: 4-byte level length + bit-packed run header.
/// Followed by `bits` (n.div_ceil(8) bytes, LSB first) and then the non-null values.
pub fn def_levels_prefix(n: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(16);
    if n == 0 {
        v.extend_from_slice(&0u32.to_le_bytes());
        return v;
    }
    let groups = n.div_ceil(8);
    let hdr = ((groups as u64) << 1) | 1;
    v.extend_from_slice(&((varint_len(hdr) + groups) as u32).to_le_bytes());
    uvarint(&mut v, hdr);
    v
}

/// Writes a one-row-group file whose column pages are the given parts, without copying
/// them into an intermediate buffer. When `num_rows == 0`, writes a clean 0-row file
/// with no row groups and no orphan data page headers.
pub fn write_columns(path: &std::path::Path, num_rows: usize, cols: &[Column]) -> std::io::Result<()> {
    use std::io::Write;
    if num_rows == 0 {
        let metas: Vec<ColMeta> = cols
            .iter()
            .map(|c| ColMeta { name: c.name, kind: c.kind, offset: 0, size: 0 })
            .collect();
        let mut foot = Vec::with_capacity(512);
        footer(&mut foot, 0, &metas);
        let mut f = std::fs::File::create(path)?;
        let bufs = [std::io::IoSlice::new(PAR1), std::io::IoSlice::new(&foot)];
        let mut slices: &mut [std::io::IoSlice] = &mut [bufs[0], bufs[1]];
        while !slices.is_empty() {
            let n = f.write_vectored(slices)?;
            if n == 0 {
                return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "write_vectored wrote 0"));
            }
            std::io::IoSlice::advance_slices(&mut slices, n);
        }
        return Ok(());
    }
    let mut headers: Vec<Vec<u8>> = Vec::with_capacity(cols.len());
    let mut metas = Vec::with_capacity(cols.len());
    let mut pos = PAR1.len() as u64;
    for c in cols {
        let page: usize = c.parts.iter().map(|p| p.len()).sum();
        let mut h = Vec::with_capacity(32);
        page_header(&mut h, page, num_rows);
        let size = (h.len() + page) as u64;
        metas.push(ColMeta {
            name: c.name,
            kind: c.kind,
            offset: pos,
            size,
        });
        pos += size;
        headers.push(h);
    }
    let mut foot = Vec::with_capacity(1024);
    footer(&mut foot, num_rows, &metas);
    let mut f = std::fs::File::create(path)?;
    let mut bufs: Vec<std::io::IoSlice> = Vec::with_capacity(2 + cols.len() * 4);
    bufs.push(std::io::IoSlice::new(PAR1));
    for (c, h) in cols.iter().zip(&headers) {
        bufs.push(std::io::IoSlice::new(h));
        for p in &c.parts {
            if !p.is_empty() {
                bufs.push(std::io::IoSlice::new(p));
            }
        }
    }
    bufs.push(std::io::IoSlice::new(&foot));
    let mut slices: &mut [std::io::IoSlice] = &mut bufs;
    while !slices.is_empty() {
        let n = f.write_vectored(slices)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write_vectored wrote 0",
            ));
        }
        std::io::IoSlice::advance_slices(&mut slices, n);
    }
    Ok(())
}
