//! Dependency-free Parquet v1 page and row group writer.

use std::collections::HashMap;
use std::hash::Hash;

const PAR1: &[u8; 4] = b"PAR1";

// parquet.thrift enum values
const T_INT32: i32 = 1;
const T_INT64: i32 = 2;
const T_BYTE_ARRAY: i32 = 6;
const REP_REQUIRED: i32 = 0;
const REP_OPTIONAL: i32 = 1;
const ENC_PLAIN: i32 = 0;
const ENC_PLAIN_DICTIONARY: i32 = 2;
const ENC_RLE: i32 = 3;
const CONV_UTF8: i32 = 0;
const CONV_DATE: i32 = 6;
const PAGE_DATA: i32 = 0;
const PAGE_DICTIONARY: i32 = 2;
const CODEC_UNCOMPRESSED: i32 = 0;
const CODEC_SNAPPY: i32 = 1;
const DICTIONARY_LIMIT: usize = 1 << 14;

// Thrift compact type ids
const CT_I32: u8 = 5;
const CT_I64: u8 = 6;
const CT_BINARY: u8 = 8;
const CT_LIST: u8 = 9;
const CT_STRUCT: u8 = 12;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Utf8,
    OptUtf8,
    Int32,
    Int64,
    Date,
}

#[derive(Clone, Copy)]
enum Compression {
    None,
    Snappy,
    ZeroWords,
}

impl Compression {
    fn parquet_codec(self) -> i32 {
        match self {
            Self::None => CODEC_UNCOMPRESSED,
            Self::Snappy | Self::ZeroWords => CODEC_SNAPPY,
        }
    }
}

#[derive(Clone)]
struct ColMeta {
    name: &'static str,
    kind: Kind,
    codec: i32,
    dictionary_offset: Option<usize>,
    data_offset: usize,
    uncompressed: usize,
    compressed: usize,
}

impl ColMeta {
    fn first_page(&self) -> usize {
        self.dictionary_offset.unwrap_or(self.data_offset)
    }
}

/// Reuses the file and page buffers across parts. Columns are added in schema order.
#[derive(Default)]
pub struct FileBuilder {
    buf: Vec<u8>,
    start: usize,
    page: Vec<u8>,
    compressed: Vec<u8>,
    indices: Vec<u16>,
    cols: Vec<ColMeta>,
    num_rows: usize,
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

impl FileBuilder {
    pub fn reset(&mut self, num_rows: usize) {
        self.num_rows = num_rows;
        self.start = 0;
        self.cols.clear();
        self.buf.clear();
        self.buf.reserve(4096 + 12 * num_rows);
        self.buf.extend_from_slice(PAR1);
    }

    pub fn col_date(&mut self, vals: &[i32]) {
        self.page.clear();
        if let Some((first, last)) = dense_indices(vals, &mut self.indices) {
            for day in first..=last {
                self.page.extend_from_slice(&(day as i32).to_le_bytes());
            }
            self.write_dictionary(
                "day",
                Kind::Date,
                (last - first + 1) as usize,
                None,
                Compression::None,
            );
        } else if let Some(dict) = integer_dictionary(
            vals,
            &mut self.indices,
            i64::from(vals.first().copied().unwrap_or_default()) - 128,
        ) {
            self.page.extend_from_slice(super::bytes_of(&dict));
            self.write_dictionary("day", Kind::Date, dict.len(), None, Compression::None);
        } else {
            self.write_plain("day", Kind::Date, super::bytes_of(vals));
        }
    }

    pub fn col_i64(&mut self, name: &'static str, vals: &[i64]) {
        self.page.clear();
        let kind = integer_kind(vals);
        if kind == Kind::Int32 && self.num_rows != 0 {
            write_i32_values(vals, &mut self.page);
            let compression = if name == "revenue_cents" {
                Compression::ZeroWords
            } else {
                Compression::Snappy
            };
            self.add_data(name, kind, compression, None, None);
        } else {
            self.write_plain(name, kind, super::bytes_of(vals));
        }
    }

    // Keep the physical schema fixed across groups, including counts above i32::MAX.
    pub fn col_u32(&mut self, name: &'static str, vals: &[u32]) {
        self.page.clear();
        if vals.is_empty() {
            self.write_plain(name, Kind::Int64, &[]);
            return;
        }
        if name == "sessions" {
            if let Some((first, last)) = dense_indices(vals, &mut self.indices) {
                for value in first..=last {
                    self.page.extend_from_slice(&value.to_le_bytes());
                }
                self.write_dictionary(
                    name, Kind::Int64, (last - first + 1) as usize, None, Compression::None,
                );
                return;
            }
        }
        if let Some(dict) = integer_dictionary(vals, &mut self.indices, 0) {
            for &value in &dict {
                self.page.extend_from_slice(&i64::from(value).to_le_bytes());
            }
            self.write_dictionary(name, Kind::Int64, dict.len(), None, Compression::None);
        } else {
            for &value in vals {
                self.page.extend_from_slice(&i64::from(value).to_le_bytes());
            }
            self.write_buffered_plain(name, Kind::Int64);
        }
    }

    pub fn col_user_revenue(&mut self, vals: &[i64]) {
        self.add_data(
            "revenue_cents", Kind::Int64, Compression::ZeroWords, None, Some(super::bytes_of(vals)),
        );
    }

    pub fn col_user_id(&mut self, plain: &[u8]) {
        if self.num_rows == 0 {
            self.write_plain("user_id", Kind::Utf8, plain);
            return;
        }

        // user_id is the first page and by far the largest. Write its Snappy
        // body into the final buffer and backfill the header immediately before
        // it once we know the compressed length. The file starts at that prefix.
        const HEADER_SPACE: usize = 64;
        debug_assert_eq!(self.buf, PAR1);
        self.buf.resize(HEADER_SPACE, 0);
        if plain.len() <= self.num_rows * 64 {
            snappy_user_ids(plain, &mut self.buf);
        } else {
            snappy(plain, &mut self.buf);
        }
        let compressed_size = self.buf.len() - HEADER_SPACE;
        self.page.clear();
        self.page.extend_from_slice(PAR1);
        page_header(
            &mut self.page,
            plain.len(),
            compressed_size,
            self.num_rows,
            PAGE_DATA,
            ENC_PLAIN,
        );
        self.start = HEADER_SPACE - self.page.len();
        self.buf[self.start..HEADER_SPACE].copy_from_slice(&self.page);
        let header = self.page.len() - PAR1.len();
        self.cols.push(ColMeta {
            name: "user_id",
            kind: Kind::Utf8,
            codec: CODEC_SNAPPY,
            dictionary_offset: None,
            data_offset: PAR1.len(),
            uncompressed: header + plain.len(),
            compressed: header + compressed_size,
        });
    }

    pub fn col_utf8<'a>(
        &mut self,
        name: &'static str,
        vals: impl Iterator<Item = &'a [u8]> + Clone,
    ) {
        self.page.clear();
        if let Some(dict) = string_dictionary(vals.clone(), &mut self.indices) {
            for value in &dict {
                plain_string(&mut self.page, value);
            }
            self.write_dictionary(name, Kind::Utf8, dict.len(), None, Compression::Snappy);
        } else {
            for value in vals {
                plain_string(&mut self.page, value);
            }
            self.write_buffered_plain(name, Kind::Utf8);
        }
    }

    pub fn col_category(&mut self, defs: &[u8], plain: &[u8]) {
        let mut remaining = plain;
        let values = std::iter::from_fn(|| {
            if remaining.is_empty() {
                return None;
            }
            let len = read_u32(remaining) as usize;
            let (value, tail) = remaining[4..].split_at(len);
            remaining = tail;
            Some(value)
        });
        self.page.clear();
        if let Some(dict) = string_dictionary(values, &mut self.indices) {
            for value in &dict {
                plain_string(&mut self.page, value);
            }
            self.write_dictionary(
                "top_category",
                Kind::OptUtf8,
                dict.len(),
                Some(defs),
                Compression::Snappy,
            );
        } else {
            definition_levels(&mut self.page, self.num_rows, defs);
            self.page.extend_from_slice(plain);
            self.write_buffered_plain("top_category", Kind::OptUtf8);
        }
    }

    fn write_dictionary(
        &mut self,
        name: &'static str,
        kind: Kind,
        count: usize,
        defs: Option<&[u8]>,
        codec: Compression,
    ) {
        let dictionary_offset = self.buf.len() - self.start;
        let uncompressed = append_page(
            &mut self.buf,
            &mut self.compressed,
            &self.page,
            codec,
            count,
            PAGE_DICTIONARY,
            ENC_PLAIN,
        );
        self.page.clear();
        if let Some(defs) = defs {
            definition_levels(&mut self.page, self.num_rows, defs);
        }
        dictionary_indices(&mut self.page, &self.indices, count);
        self.add_data(
            name,
            kind,
            codec,
            Some((dictionary_offset, uncompressed)),
            None,
        );
    }

    fn write_buffered_plain(&mut self, name: &'static str, kind: Kind) {
        self.add_data(name, kind, Compression::Snappy, None, None);
    }

    fn write_plain(&mut self, name: &'static str, kind: Kind, data: &[u8]) {
        self.add_data(name, kind, Compression::Snappy, None, Some(data));
    }

    fn add_data(
        &mut self,
        name: &'static str,
        kind: Kind,
        codec: Compression,
        dictionary: Option<(usize, usize)>,
        external_page: Option<&[u8]>,
    ) {
        let data_offset = self.buf.len() - self.start;
        let mut uncompressed = 0;
        if self.num_rows != 0 {
            let encoding = if dictionary.is_some() {
                ENC_PLAIN_DICTIONARY
            } else {
                ENC_PLAIN
            };
            uncompressed = append_page(
                &mut self.buf,
                &mut self.compressed,
                external_page.unwrap_or(&self.page),
                codec,
                self.num_rows,
                PAGE_DATA,
                encoding,
            );
        }
        self.cols.push(ColMeta {
            name,
            kind,
            codec: codec.parquet_codec(),
            dictionary_offset: dictionary.map(|(offset, _)| offset),
            data_offset,
            uncompressed: uncompressed + dictionary.map_or(0, |(_, size)| size),
            compressed: self.buf.len()
                - self.start
                - dictionary.map_or(data_offset, |(offset, _)| offset),
        });
    }

    pub fn finish(&mut self) -> &[u8] {
        let groups = std::iter::once((self.num_rows, self.cols.as_slice()))
            .take(usize::from(self.num_rows != 0));
        footer(&mut self.buf, self.num_rows, &self.cols, groups);
        &self.buf[self.start..]
    }

    pub fn into_bytes(mut self) -> Vec<u8> {
        self.finish();
        self.buf.drain(..self.start);
        self.buf
    }
}

/// Writes independently encoded row groups while retaining only footer metadata.
pub struct RowGroupFile {
    file: std::fs::File,
    end: usize,
    rows: usize,
    groups: Vec<(usize, Vec<ColMeta>)>,
}

impl RowGroupFile {
    pub fn create(path: &std::path::Path) -> std::io::Result<Self> {
        Ok(Self {
            file: std::fs::File::create(path)?,
            end: 0,
            rows: 0,
            groups: Vec::new(),
        })
    }

    pub fn append(&mut self, group: &FileBuilder) -> std::io::Result<()> {
        use std::io::Write;
        debug_assert!(self.groups.first().is_none_or(|(_, cols)| {
            cols.len() == group.cols.len()
                && cols.iter().zip(&group.cols).all(|(a, b)| a.kind == b.kind && a.name == b.name)
        }));
        let bytes = &group.buf[group.start..];
        let skip = if self.groups.is_empty() { 0 } else { PAR1.len() };
        self.file.write_all(&bytes[skip..])?;
        let mut cols = group.cols.clone();
        if skip != 0 {
            let delta = self.end - skip;
            for col in &mut cols {
                col.data_offset += delta;
                col.dictionary_offset = col.dictionary_offset.map(|offset| offset + delta);
            }
        }
        self.end += bytes.len() - skip;
        self.rows += group.num_rows;
        self.groups.push((group.num_rows, cols));
        Ok(())
    }

    pub fn finish(mut self) -> std::io::Result<()> {
        use std::io::Write;
        let mut tail = Vec::new();
        let groups = self.groups.iter().map(|(rows, cols)| (*rows, cols.as_slice()));
        footer(&mut tail, self.rows, &self.groups[0].1, groups);
        self.file.write_all(&tail)
    }
}

fn integer_kind(vals: &[i64]) -> Kind {
    let outside = vals.iter().fold(0, |bits, &value| {
        bits | (value.wrapping_sub(i32::MIN as i64) as u64 >> 32)
    });
    if outside == 0 {
        Kind::Int32
    } else {
        Kind::Int64
    }
}

fn write_i32_values(vals: &[i64], out: &mut Vec<u8>) {
    let start = out.len();
    out.resize(start + vals.len() * 4, 0);
    for (&value, bytes) in vals.iter().zip(out[start..].chunks_exact_mut(4)) {
        bytes.copy_from_slice(&(value as i32).to_le_bytes());
    }
}

fn plain_string(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}

fn string_dictionary<'a>(
    vals: impl Iterator<Item = &'a [u8]>,
    indices: &mut Vec<u16>,
) -> Option<Vec<&'a [u8]>> {
    let mut ids = HashMap::new();
    let mut dict = Vec::new();
    // Keep common strings out of the hash table's general lookup path.
    let mut recent: [(&[u8], u16); 128] = [(&[], u16::MAX); 128];
    indices.clear();
    for value in vals {
        let short_hash = if value.len() >= 4 {
            read_u32(value).wrapping_add(read_u32(&value[value.len() - 4..]).rotate_left(11))
        } else {
            value.iter().fold(value.len() as u32, |word, &byte| {
                (word << 8) | u32::from(byte)
            })
        };
        let slot = &mut recent[(short_hash.wrapping_mul(0x1e35_a7bd) >> 25) as usize];
        if slot.1 == u16::MAX || slot.0 != value {
            let index = match ids.entry(value) {
                std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    if dict.len() == DICTIONARY_LIMIT {
                        return None;
                    }
                    let index = dict.len() as u16;
                    entry.insert(index);
                    dict.push(value);
                    index
                }
            };
            *slot = (value, index);
        }
        indices.push(slot.1);
    }
    (!dict.is_empty()).then_some(dict)
}

/// A small dense interval can use each value's distance from the minimum as its index.
fn dense_indices<T: Copy + Ord + Into<i64>>(
    vals: &[T],
    indices: &mut Vec<u16>,
) -> Option<(i64, i64)> {
    let &first = vals.first()?;
    let (min, max) = vals.iter().fold((first, first), |(min, max), &value| {
        (min.min(value), max.max(value))
    });
    let min = min.into();
    let max = max.into();
    let span = max.checked_sub(min)?;
    if span >= 256 || span as usize >= vals.len() {
        return None;
    }
    indices.clear();
    indices.extend(vals.iter().map(|&value| (value.into() - min) as u16));
    Some((min, max))
}

fn integer_dictionary<T: Copy + Into<i64> + Eq + Hash>(
    vals: &[T],
    indices: &mut Vec<u16>,
    base: i64,
) -> Option<Vec<T>> {
    if vals.is_empty() {
        return None;
    }
    // Days and small counts usually occupy a short interval. Hash only outliers.
    let mut slots = [u16::MAX; 4096];
    let mut outliers = HashMap::new();
    let mut dict = Vec::new();
    indices.resize(vals.len(), 0);
    for (encoded, &value) in indices.iter_mut().zip(vals) {
        let offset = value.into().wrapping_sub(base) as usize;
        let index = if let Some(index) = slots.get_mut(offset) {
            index
        } else {
            outliers.entry(value).or_insert(u16::MAX)
        };
        if *index == u16::MAX {
            if dict.len() == DICTIONARY_LIMIT {
                return None;
            }
            *index = dict.len() as u16;
            dict.push(value);
        }
        *encoded = *index;
    }
    Some(dict)
}

/// Data page v1 definition levels carry a length prefix. Each byte in `bits` is
/// already one group of eight levels in little-endian bit order.
fn definition_levels(out: &mut Vec<u8>, rows: usize, bits: &[u8]) {
    let start = out.len();
    out.extend_from_slice(&[0; 4]);
    if rows != 0 {
        if bits.iter().all(|&byte| byte == 0) {
            uvarint(out, (rows as u64) << 1);
            out.push(0);
        } else {
            uvarint(out, (bits.len() as u64) << 1 | 1);
            out.extend_from_slice(bits);
        }
    }
    let size = (out.len() - start - 4) as u32;
    out[start..start + 4].copy_from_slice(&size.to_le_bytes());
}

fn dictionary_indices(out: &mut Vec<u8>, indices: &[u16], count: usize) {
    let width = usize::BITS - (count - 1).leading_zeros();
    out.push(width as u8);
    if width == 0 {
        uvarint(out, (indices.len() as u64) << 1);
        return;
    }
    let groups = indices.len().div_ceil(8);
    uvarint(out, (groups as u64) << 1 | 1);
    out.reserve(groups * width as usize);
    match width {
        1 => pack_groups::<1>(out, indices),
        2 => pack_groups::<2>(out, indices),
        3 => pack_groups::<3>(out, indices),
        4 => pack_groups::<4>(out, indices),
        5 => pack_groups::<5>(out, indices),
        6 => pack_groups::<6>(out, indices),
        7 => pack_groups::<7>(out, indices),
        8 => pack_groups::<8>(out, indices),
        9 => pack_groups::<9>(out, indices),
        10 => pack_groups::<10>(out, indices),
        11 => pack_groups::<11>(out, indices),
        12 => pack_groups::<12>(out, indices),
        13 => pack_groups::<13>(out, indices),
        14 => pack_groups::<14>(out, indices),
        _ => unreachable!("dictionary exceeds limit"),
    }
}

fn pack_groups<const WIDTH: usize>(out: &mut Vec<u8>, indices: &[u16]) {
    let start = out.len();
    out.resize(start + indices.len().div_ceil(8) * WIDTH, 0);
    let (groups, tail) = indices.as_chunks::<8>();
    let (full_bytes, tail_bytes) = out[start..].split_at_mut(groups.len() * WIDTH);
    for (bytes, group) in full_bytes.chunks_exact_mut(WIDTH).zip(groups) {
        let packed = pack_eight::<WIDTH>(group);
        bytes.copy_from_slice(&packed.to_le_bytes()[..WIDTH]);
    }
    if !tail.is_empty() {
        let mut padded = [0; 8];
        padded[..tail.len()].copy_from_slice(tail);
        let packed = pack_eight::<WIDTH>(&padded);
        tail_bytes.copy_from_slice(&packed.to_le_bytes()[..WIDTH]);
    }
}

#[inline]
fn pack_eight<const WIDTH: usize>(indices: &[u16; 8]) -> u128 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        use std::arch::x86_64::_pext_u64;
        let mask = ((1u64 << WIDTH) - 1) * 0x0001_0001_0001_0001;
        let bytes = super::bytes_of(indices);
        // SAFETY: the compile-time target feature guarantees BMI2 support.
        unsafe {
            let lo = _pext_u64(read_u64(bytes), mask) as u128;
            let hi = _pext_u64(read_u64(&bytes[8..]), mask) as u128;
            lo | (hi << (4 * WIDTH))
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let mut packed = 0u128;
        for (i, &index) in indices.iter().enumerate() {
            packed |= (index as u128) << (i * WIDTH);
        }
        packed
    }
}

fn append_page(
    out: &mut Vec<u8>,
    compressed: &mut Vec<u8>,
    data: &[u8],
    codec: Compression,
    num_values: usize,
    page_type: i32,
    encoding: i32,
) -> usize {
    compressed.clear();
    let stored = match codec {
        Compression::None => data,
        Compression::Snappy => {
            snappy(data, compressed);
            compressed
        }
        Compression::ZeroWords => {
            snappy_zero_words(data, compressed);
            compressed
        }
    };
    let start = out.len();
    page_header(
        out,
        data.len(),
        stored.len(),
        num_values,
        page_type,
        encoding,
    );
    let header_len = out.len() - start;
    out.extend_from_slice(stored);
    header_len + data.len()
}

fn page_header(
    out: &mut Vec<u8>,
    uncompressed: usize,
    compressed: usize,
    num_values: usize,
    page_type: i32,
    encoding: i32,
) {
    let mut tw = Tw::new(out);
    tw.i32(1, page_type);
    tw.i32(
        2,
        i32::try_from(uncompressed).expect("parquet page too large"),
    );
    tw.i32(
        3,
        i32::try_from(compressed).expect("compressed parquet page too large"),
    );
    tw.begin(Some(if page_type == PAGE_DICTIONARY { 7 } else { 5 }));
    tw.i32(1, num_values as i32);
    tw.i32(2, encoding);
    if page_type == PAGE_DATA {
        tw.i32(3, ENC_RLE);
        tw.i32(4, ENC_RLE);
    }
    tw.end();
    tw.out.push(0); // end PageHeader
}

#[inline]
fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes[..4].try_into().unwrap())
}

#[inline]
fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[..8].try_into().unwrap())
}

/// Raw Snappy block with 64 KiB matching windows and a 4K-entry position table.
fn snappy(input: &[u8], out: &mut Vec<u8>) {
    uvarint(out, input.len() as u64);
    for block in input.chunks(1 << 16) {
        snappy_block(block, out);
    }
}

fn snappy_block(input: &[u8], out: &mut Vec<u8>) {
    let mut table = [0u16; 1 << 12];
    let mut literal = 0;
    let mut pos = 1;
    let mut skip = 32;
    while pos + 4 <= input.len() {
        let word = read_u32(&input[pos..]);
        let hash = (word.wrapping_mul(0x1e35_a7bd) >> 20) as usize;
        let prev = table[hash] as usize;
        table[hash] = pos as u16;
        if read_u32(&input[prev..]) != word {
            pos += skip >> 5;
            skip += 1;
            continue;
        }
        snappy_literal(&input[literal..pos], out);
        let start = pos;
        pos += 4;
        while pos + 8 <= input.len() {
            let matched_pos = prev + pos - start;
            let left = read_u64(&input[pos..]);
            let right = read_u64(&input[matched_pos..]);
            let diff = left ^ right;
            if diff != 0 {
                pos += diff.trailing_zeros() as usize / 8;
                break;
            }
            pos += 8;
        }
        while pos < input.len() && input[pos] == input[prev + pos - start] {
            pos += 1;
        }
        snappy_copy(start - prev, pos - start, out);
        literal = pos;
        skip = 32;
    }
    snappy_literal(&input[literal..], out);
}

/// The PLAIN ID stream places identical length-prefixed records side by side.
fn snappy_user_ids(input: &[u8], out: &mut Vec<u8>) {
    out.reserve(input.len() / 2);
    uvarint(out, input.len() as u64);
    let mut literal = 0;
    let mut pos = 0;
    while pos < input.len() {
        let len = 4 + read_u32(&input[pos..]) as usize;
        let end = pos + len;
        let record = &input[pos..end];
        pos = end;
        if (8..=16).contains(&len) {
            let first = read_u64(record);
            let last = read_u64(&record[len - 8..]);
            while pos + len <= input.len()
                && read_u64(&input[pos..]) == first
                && read_u64(&input[pos + len - 8..]) == last
            {
                pos += len;
            }
        } else if len == 37 {
            let a = read_u64(record);
            let b = read_u64(&record[8..]);
            let c = read_u64(&record[16..]);
            let d = read_u64(&record[24..]);
            let e = read_u64(&record[29..]);
            while pos + 37 <= input.len()
                && read_u64(&input[pos..]) == a
                && read_u64(&input[pos + 8..]) == b
                && read_u64(&input[pos + 16..]) == c
                && read_u64(&input[pos + 24..]) == d
                && read_u64(&input[pos + 29..]) == e
            {
                pos += 37;
            }
        } else if len <= u16::MAX as usize {
            while input[pos..].starts_with(record) {
                pos += len;
            }
        }
        if pos != end {
            snappy_literal_and_copy(&input[literal..end], len, pos - end, out);
            literal = pos;
        }
    }
    snappy_literals(&input[literal..], out);
}

/// Copy runs of zero words in PLAIN integer pages.
fn snappy_zero_words(input: &[u8], out: &mut Vec<u8>) {
    out.reserve(input.len() * 3 / 4);
    uvarint(out, input.len() as u64);
    let (pairs, _) = input.as_chunks::<8>();
    let mut pairs = pairs.iter().enumerate();
    let mut literal = 0;
    while let Some((start, _)) = pairs.find(|(_, pair)| **pair == [0; 8]) {
        let end = pairs
            .find(|(_, pair)| **pair != [0; 8])
            .map_or(input.len() / 8, |(index, _)| index)
            * 8;
        let copied = start * 8 + 4;
        snappy_literal_and_copy(&input[literal..copied], 4, end - copied, out);
        literal = end;
    }
    snappy_literals(&input[literal..], out);
}

/// Short literals and short copies account for most ID and zero-word runs.
#[inline]
fn snappy_literal_and_copy(literal: &[u8], offset: usize, matched: usize, out: &mut Vec<u8>) {
    if !(1..=60).contains(&literal.len()) || matched > 64 {
        snappy_literals(literal, out);
        snappy_copy(offset, matched, out);
        return;
    }
    let tag = if (4..=11).contains(&matched) && offset < 2048 {
        let tag = 1 | ((matched as u32 - 4) << 2) | ((offset as u32 >> 8) << 5);
        (tag | ((offset as u32 & 255) << 8), 2)
    } else {
        (2 | ((matched as u32 - 1) << 2) | ((offset as u32) << 8), 3)
    };
    let start = out.len();
    out.reserve(1 + literal.len() + 4);
    // SAFETY: reserve provides space for the header, literal, and the full
    // four-byte tag store. Only the tag's two or three encoded bytes are exposed.
    unsafe {
        let dst = out.as_mut_ptr().add(start);
        dst.write(((literal.len() as u8) - 1) << 2);
        std::ptr::copy_nonoverlapping(literal.as_ptr(), dst.add(1), literal.len());
        dst.add(1 + literal.len())
            .cast::<u32>()
            .write_unaligned(tag.0.to_le());
        out.set_len(start + 1 + literal.len() + tag.1);
    }
}

fn snappy_literals(bytes: &[u8], out: &mut Vec<u8>) {
    for literal in bytes.chunks(1 << 16) {
        snappy_literal(literal, out);
    }
}

fn snappy_literal(bytes: &[u8], out: &mut Vec<u8>) {
    if bytes.is_empty() {
        return;
    }
    let len = bytes.len() - 1;
    if len < 60 {
        out.push((len as u8) << 2);
    } else {
        let width = if len <= u8::MAX as usize { 1 } else { 2 };
        out.push((59 + width as u8) << 2);
        out.extend_from_slice(&len.to_le_bytes()[..width]);
    }
    out.extend_from_slice(bytes);
}

fn snappy_copy(offset: usize, mut len: usize, out: &mut Vec<u8>) {
    let [low, high] = (offset as u16).to_le_bytes();
    while len > 64 {
        out.extend_from_slice(&[(63 << 2) | 2, low, high]);
        len -= 64;
    }
    if (4..=11).contains(&len) && offset < 2048 {
        out.extend_from_slice(&[1 | (((len - 4) as u8) << 2) | (high << 5), low]);
    } else {
        out.extend_from_slice(&[(((len - 1) as u8) << 2) | 2, low, high]);
    }
}

/// Appends FileMetaData, its length and the trailing magic.
fn footer<'a>(
    out: &mut Vec<u8>,
    num_rows: usize,
    cols: &[ColMeta],
    groups: impl ExactSizeIterator<Item = (usize, &'a [ColMeta])>,
) {
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
                Kind::Int32 | Kind::Date => (T_INT32, REP_REQUIRED),
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
                Kind::Int32 | Kind::Int64 => {}
            }
            tw.end();
        }
        tw.i64(3, num_rows);
        tw.list(4, CT_STRUCT, groups.len());
        for (num_rows, cols) in groups {
            let num_rows = num_rows as i64;
            tw.begin(None); // RowGroup
            tw.list(1, CT_STRUCT, cols.len());
            let mut uncompressed = 0i64;
            let mut compressed = 0i64;
            for c in cols.iter() {
                uncompressed += c.uncompressed as i64;
                compressed += c.compressed as i64;
                tw.begin(None); // ColumnChunk
                tw.i64(2, c.first_page() as i64);
                tw.begin(Some(3)); // ColumnMetaData
                let ty = match c.kind {
                    Kind::Utf8 | Kind::OptUtf8 => T_BYTE_ARRAY,
                    Kind::Int64 => T_INT64,
                    Kind::Int32 | Kind::Date => T_INT32,
                };
                tw.i32(1, ty);
                tw.list(2, CT_I32, 2 + usize::from(c.dictionary_offset.is_some()));
                tw.raw_i32(ENC_PLAIN);
                tw.raw_i32(ENC_RLE);
                if c.dictionary_offset.is_some() {
                    tw.raw_i32(ENC_PLAIN_DICTIONARY);
                }
                tw.list(3, CT_BINARY, 1);
                tw.raw_binary(c.name.as_bytes());
                tw.i32(4, c.codec);
                tw.i64(5, num_rows);
                tw.i64(6, c.uncompressed as i64);
                tw.i64(7, c.compressed as i64);
                tw.i64(9, c.data_offset as i64);
                if let Some(offset) = c.dictionary_offset {
                    tw.i64(11, offset as i64);
                }
                tw.end(); // ColumnMetaData
                tw.end(); // ColumnChunk
            }
            tw.i64(2, uncompressed);
            tw.i64(3, num_rows);
            tw.i64(5, cols[0].first_page() as i64);
            tw.i64(6, compressed);
            tw.end(); // RowGroup
        }
        tw.binary(6, b"shopping-sessions native writer");
        tw.out.push(0); // end FileMetaData
    }
    let footer_len = (out.len() - footer_start) as u32;
    out.extend_from_slice(&footer_len.to_le_bytes());
    out.extend_from_slice(PAR1);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_snappy(mut block: &[u8]) -> Vec<u8> {
        let mut expected_len = 0;
        for shift in (0..).step_by(7) {
            let (&byte, rest) = block.split_first().unwrap();
            block = rest;
            expected_len |= ((byte & 127) as usize) << shift;
            if byte < 128 {
                break;
            }
        }
        let mut out = Vec::with_capacity(expected_len);
        while let Some((&tag, rest)) = block.split_first() {
            block = rest;
            match tag & 3 {
                0 => {
                    let mut len = (tag >> 2) as usize;
                    if len >= 60 {
                        let width = len - 59;
                        len = 0;
                        for (i, &byte) in block[..width].iter().enumerate() {
                            len |= (byte as usize) << (8 * i);
                        }
                        block = &block[width..];
                    }
                    let (literal, rest) = block.split_at(len + 1);
                    out.extend_from_slice(literal);
                    block = rest;
                }
                copy => {
                    let (len, offset) = if copy == 1 {
                        let offset = (((tag & 224) as usize) << 3) | block[0] as usize;
                        block = &block[1..];
                        (4 + ((tag >> 2) & 7) as usize, offset)
                    } else {
                        assert_eq!(copy, 2);
                        let offset = u16::from_le_bytes(block[..2].try_into().unwrap());
                        block = &block[2..];
                        (1 + (tag >> 2) as usize, offset as usize)
                    };
                    assert!((1..=out.len()).contains(&offset));
                    for _ in 0..len {
                        out.push(out[out.len() - offset]);
                    }
                }
            }
        }
        assert_eq!(out.len(), expected_len);
        out
    }

    #[test]
    fn snappy_roundtrip_across_windows() {
        let mut state = 0xa706_78fb_u32;
        let random: Vec<_> = (0..150_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect();
        let mut patterned = random[..8192].repeat(20);
        patterned.extend_from_slice(&[0; 32768]);
        patterned.extend_from_slice(b"123ab123ab3bab");
        let mut encoded = Vec::new();
        for input in [
            &[][..],
            &b"abc"[..],
            &random[..60],
            &random[..61],
            &random[..256],
            &random[..257],
            &random,
            &patterned,
        ] {
            encoded.clear();
            snappy(input, &mut encoded);
            assert_eq!(decode_snappy(&encoded), input);
        }
        assert!(encoded.len() < patterned.len() / 4);
    }

    #[test]
    fn snappy_user_ids_roundtrip_with_mixed_record_lengths() {
        let mut input = Vec::new();
        let long_id = vec![b'x'; 70_000];
        for id in [
            &b"u"[..],
            &b"u"[..],
            &b"u1231234"[..],
            &b"u1231234"[..],
            &b"u1231294"[..],
            &b"u1241234"[..],
            &b"other length"[..],
            &b""[..],
            &b""[..],
            &long_id,
            &long_id,
        ] {
            plain_string(&mut input, id);
        }
        let mut encoded = Vec::new();
        snappy_user_ids(&input, &mut encoded);
        assert_eq!(decode_snappy(&encoded), input);
    }

    #[test]
    fn snappy_zero_words_preserves_runs_at_either_boundary() {
        let mut encoded = Vec::new();
        for vals in [
            &[][..],
            &[0][..],
            &[3, 0, 0, 0, 3, 4, 0, 0][..],
            &[0, 0, 1, 2, 0, 3][..],
            &[0; 257][..],
            &[3; 100][..],
        ] {
            let input = super::super::bytes_of(vals);
            encoded.clear();
            snappy_zero_words(input, &mut encoded);
            assert_eq!(decode_snappy(&encoded), input);
        }
    }

    #[test]
    fn all_null_uses_plain_with_only_definition_levels() {
        let mut file = FileBuilder::default();
        file.reset(12);
        file.col_category(&[0, 0], &[]);
        let col = &file.cols[0];
        assert_eq!(col.kind, Kind::OptUtf8);
        assert_eq!(col.dictionary_offset, None);
        assert_eq!(decode_snappy(&file.compressed), [2, 0, 0, 0, 24, 0]);
    }

    #[test]
    fn one_value_uses_zero_width_rle_indices() {
        let mut indices = Vec::new();
        let dict = integer_dictionary(&[123; 19], &mut indices, 0).unwrap();
        assert_eq!(dict, [123]);
        let mut encoded = Vec::new();
        dictionary_indices(&mut encoded, &indices, dict.len());
        assert_eq!(encoded, [0, 38]);
    }

    #[test]
    fn bit_packed_indices_preserve_values_and_pad_final_group() {
        for width in 1..=14 {
            let mask = (1 << width) - 1;
            let indices: Vec<_> = (1..=19).map(|i| (i * 397) & mask).collect();
            let mut encoded = Vec::new();
            dictionary_indices(&mut encoded, &indices, 1 << width);
            assert_eq!(&encoded[..2], &[width as u8, 7]);
            for i in 0..24 {
                let mut index = 0;
                for j in 0..width {
                    let bit = i * width + j;
                    index |= (((encoded[2 + bit / 8] >> (bit % 8)) & 1) as u16) << j;
                }
                assert_eq!(index, indices.get(i).copied().unwrap_or_default());
            }
        }
    }

    #[test]
    fn selects_i32_only_when_both_bounds_fit() {
        let mut file = FileBuilder::default();
        for (value, expected) in [
            (i32::MIN as i64 - 1, Kind::Int64),
            (i32::MIN as i64, Kind::Int32),
            (i32::MAX as i64, Kind::Int32),
            (i32::MAX as i64 + 1, Kind::Int64),
        ] {
            file.reset(2);
            file.col_i64("sessions", &[0, value]);
            assert_eq!(file.cols[0].kind, expected);
            assert_eq!(file.cols[0].dictionary_offset, None);
        }
    }

    #[test]
    fn dictionary_over_limit_falls_back_to_plain_snappy() {
        let mut vals: Vec<_> = (0..DICTIONARY_LIMIT as u32).collect();
        vals.push(u32::MAX);
        let mut file = FileBuilder::default();
        file.reset(DICTIONARY_LIMIT);
        file.col_u32("events", &vals[..DICTIONARY_LIMIT]);
        assert!(file.cols[0].dictionary_offset.is_some());
        file.reset(vals.len());
        file.col_u32("events", &vals);
        assert_eq!(file.cols[0].kind, Kind::Int64);
        assert_eq!(file.cols[0].dictionary_offset, None);
        assert_eq!(file.cols[0].codec, CODEC_SNAPPY);
        let expected: Vec<_> = vals
            .iter()
            .flat_map(|&value| i64::from(value).to_le_bytes())
            .collect();
        assert_eq!(decode_snappy(&file.compressed), expected);
    }

    #[test]
    fn category_dictionary_over_limit_keeps_plain_values_and_levels() {
        let mut plain = Vec::new();
        for category in 0..=DICTIONARY_LIMIT {
            plain_string(&mut plain, category.to_string().as_bytes());
        }
        let mut file = FileBuilder::default();
        file.reset(DICTIONARY_LIMIT + 1);
        let defs = vec![u8::MAX; (DICTIONARY_LIMIT + 1).div_ceil(8)];
        file.col_category(&defs, &plain);
        assert_eq!(file.cols[0].dictionary_offset, None);
        assert_eq!(file.cols[0].codec, CODEC_SNAPPY);
        let page = decode_snappy(&file.compressed);
        let prefix = 4 + read_u32(&page) as usize;
        assert_eq!(&page[prefix - defs.len()..prefix], defs);
        assert_eq!(&page[prefix..], plain);
    }

    #[test]
    fn empty_file_has_footer_immediately_after_magic() {
        let mut file = FileBuilder::default();
        file.reset(0);
        file.col_date(&[]);
        file.col_u32("events", &[]);
        file.col_category(&[], &[]);
        let bytes = file.finish();
        let footer_size = read_u32(&bytes[bytes.len() - 8..]) as usize;
        assert_eq!(&bytes[..4], PAR1);
        assert_eq!(bytes.len(), 4 + footer_size + 8);
        assert_eq!(&bytes[bytes.len() - 4..], PAR1);
    }
}
