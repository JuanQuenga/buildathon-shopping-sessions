//! Bounded input buffers and asynchronous CSV reads for the fast ingest path.
use super::par;
use super::scan::{self, Chunk, FileInfo, Kind};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Barrier, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

const ALIGN: usize = 4096;
const EXTRA: usize = 4096;
const BUFFER_SLACK: usize = 64 << 10;
const IO_CHUNK_BYTES: usize = 4 << 20;
const READERS: usize = 24;
const POOL_BUFS: usize = 32;
const O_DIRECT: i32 = 0o40000;
const EINVAL: i32 = 22;

pub(super) fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
}

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
}

pub(super) struct AlignedBuf {
    ptr: NonNull<u8>,
    cap: usize,
}

// The mapping is owned by one reader, queue, or parser at a time.
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    fn new(min_cap: usize) -> io::Result<Self> {
        let cap = min_cap.next_multiple_of(ALIGN);
        // PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS.
        let ptr = unsafe { mmap(std::ptr::null_mut(), cap, 3, 0x22, -1, 0) };
        if ptr as isize == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            ptr: NonNull::new(ptr).expect("null mmap"),
            cap,
        })
    }

    fn grow(&mut self, min_cap: usize, used: usize) -> io::Result<()> {
        if min_cap > self.cap {
            let mut larger = Self::new(min_cap)?;
            larger
                .as_mut_slice(used)
                .copy_from_slice(self.as_slice(used));
            *self = larger;
        }
        Ok(())
    }

    pub(super) fn as_slice(&self, len: usize) -> &[u8] {
        assert!(len <= self.cap);
        // Anonymous mmap zero-initializes the mapping. No writer shares this buffer.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), len) }
    }

    fn as_mut_slice(&mut self, len: usize) -> &mut [u8] {
        assert!(len <= self.cap);
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { munmap(self.ptr.as_ptr(), self.cap) };
    }
}

struct DirectFile {
    handle: File,
    disabled: AtomicBool,
}

impl DirectFile {
    fn open(path: &Path) -> io::Result<Option<Self>> {
        match OpenOptions::new()
            .read(true)
            .custom_flags(O_DIRECT)
            .open(path)
        {
            Ok(handle) => Ok(Some(Self {
                handle,
                disabled: AtomicBool::new(false),
            })),
            Err(err) if err.raw_os_error() == Some(EINVAL) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

struct Stats {
    start: Instant,
    first_byte: AtomicU64,
    read_ns: AtomicU64,
    parse_wait_ns: AtomicU64,
    bytes: AtomicU64,
    direct_read: AtomicBool,
}

impl Stats {
    fn new(start: Instant) -> Self {
        Self {
            start,
            first_byte: AtomicU64::new(u64::MAX),
            read_ns: AtomicU64::new(0),
            parse_wait_ns: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            direct_read: AtomicBool::new(false),
        }
    }

    fn read(&self, file: &File, buf: &mut [u8], offset: u64, direct: bool) -> io::Result<usize> {
        let start = self.start.elapsed();
        let result = file.read_at(buf, offset);
        let end = self.start.elapsed();
        self.read_ns
            .fetch_add((end - start).as_nanos() as u64, Relaxed);
        if let Ok(n) = result {
            if n != 0 {
                self.first_byte.fetch_min(end.as_nanos() as u64, Relaxed);
                self.bytes.fetch_add(n as u64, Relaxed);
                if direct {
                    self.direct_read.store(true, Relaxed);
                }
            }
        }
        result
    }
}

/// Fill an aligned range, including a possibly short final sector at EOF.
/// An unaligned short read mid-file resumes through the buffered descriptor.
fn read_range(
    file: &FileInfo,
    direct: Option<&DirectFile>,
    mut offset: u64,
    mut dst: &mut [u8],
    stats: &Stats,
) -> io::Result<()> {
    let mut direct = direct.filter(|file| !file.disabled.load(Relaxed));
    while !dst.is_empty() && offset < file.len {
        let result = if let Some(direct_file) = direct {
            let result = stats.read(&direct_file.handle, dst, offset, true);
            if matches!(&result, Err(err) if err.raw_os_error() == Some(EINVAL)) {
                direct_file.disabled.store(true, Relaxed);
                direct = None;
                continue;
            }
            result
        } else {
            let remaining = dst.len().min((file.len - offset) as usize);
            stats.read(&file.handle, &mut dst[..remaining], offset, false)
        };
        match result {
            Ok(0) => return Err(ErrorKind::UnexpectedEof.into()),
            Ok(n) => {
                dst = &mut dst[n..];
                offset += n as u64;
                if n % ALIGN != 0 {
                    direct = None;
                }
            }
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// A row belongs to the chunk containing its first byte. Use the preceding
/// byte to detect a chunk starting exactly on a row boundary.
fn read_chunk_aligned(
    file: &FileInfo,
    direct: Option<&DirectFile>,
    c: Chunk,
    buf: &mut AlignedBuf,
    stats: &Stats,
) -> io::Result<(usize, usize)> {
    if c.start == c.end {
        buf.as_mut_slice(scan::PAD).fill(0);
        return Ok((0, 0));
    }

    let rs = c.start.saturating_sub(1);
    let rs_aligned = rs & !(ALIGN as u64 - 1);
    let skip = (rs - rs_aligned) as usize;
    let own_end = (c.end - rs_aligned) as usize;
    let eof = (file.len - rs_aligned) as usize;
    let mut want = (own_end + EXTRA).min(eof).next_multiple_of(ALIGN);
    buf.grow(want + scan::PAD + 1, 0)?;
    read_range(file, direct, rs_aligned, buf.as_mut_slice(want), stats)?;

    // If there is no LF before the last nominal byte, the whole chunk is
    // inside a header or a row owned by an earlier chunk.
    let lo = scan::memchr(b'\n', &buf.as_slice(own_end - 1)[skip..])
        .map_or(own_end, |pos| skip + pos + 1);
    if lo == own_end {
        buf.as_mut_slice(scan::PAD).fill(0);
        return Ok((0, 0));
    }

    let mut tail_start = own_end - 1;
    let hi = loop {
        let end = want.min(eof);
        if let Some(pos) = scan::memchr(b'\n', &buf.as_slice(end)[tail_start..]) {
            break tail_start + pos + 1;
        }
        if end == eof {
            buf.as_mut_slice(end + 1)[end] = b'\n';
            break end + 1;
        }
        let next = (want * 2).min(eof.next_multiple_of(ALIGN));
        buf.grow(next + scan::PAD + 1, want)?;
        read_range(
            file,
            direct,
            rs_aligned + want as u64,
            &mut buf.as_mut_slice(next)[want..],
            stats,
        )?;
        tail_start = want;
        want = next;
    };
    buf.as_mut_slice(hi + scan::PAD)[hi..].fill(0);
    Ok((lo, hi))
}

pub(super) struct FilledChunk {
    pub(super) chunk_idx: usize,
    pub(super) buf: AlignedBuf,
    pub(super) lo: usize,
    pub(super) hi: usize,
}

struct Source {
    files: Vec<FileInfo>,
    direct: Vec<Option<DirectFile>>,
    chunks: Vec<Chunk>,
    chunk_bytes: usize,
    readers: usize,
    bufs: usize,
    buffer_cap: usize,
    stats: Stats,
}

impl Source {
    fn advise(&self, index: usize) {
        let file = &self.files[index];
        par::fadvise_range(file.handle.as_raw_fd(), 0, file.len);
    }
}

pub(super) struct Input {
    source: Arc<Source>,
    link_chunks: Vec<Chunk>,
    stream: Option<ChunkStream>,
    drop_threads: usize,
}

impl Input {
    pub(super) fn open(data: &Path, threads: usize, start: Instant) -> Self {
        let odirect = std::env::var("INGEST_ODIRECT").as_deref() == Ok("1");
        let mut files = Vec::new();
        let mut direct = Vec::new();
        for (subdir, kind) in [
            ("links", Kind::Links),
            ("events", Kind::Events),
            ("clicks", Kind::Clicks),
        ] {
            for path in super::small::list_csv(data, subdir) {
                let handle = File::open(&path).expect("open csv");
                let len = handle.metadata().map(|m| m.len()).unwrap_or(0);
                files.push(FileInfo { handle, len, kind });
                direct.push(if odirect && kind != Kind::Links {
                    DirectFile::open(&path).expect("open csv with O_DIRECT")
                } else {
                    None
                });
            }
        }

        let chunk_bytes =
            env_usize("INGEST_IO_CHUNK_KB").map_or(IO_CHUNK_BYTES, |kb| kb.clamp(64, 16384) << 10);
        let (link_chunks, chunks): (Vec<_>, Vec<_>) = scan::plan(&files, chunk_bytes as u64)
            .into_iter()
            .partition(|c| files[c.file as usize].kind == Kind::Links);
        let readers = env_usize("INGEST_READERS")
            .map_or(READERS, |n| n.clamp(1, 64))
            .min(chunks.len());
        let bufs = env_usize("INGEST_IO_BUFS")
            .map_or(POOL_BUFS, |n| n.clamp(4, 64))
            .min(chunks.len());
        let total_bytes: u64 = files
            .iter()
            .filter(|f| f.kind != Kind::Links)
            .map(|f| f.len)
            .sum();
        let buffer_bytes = if total_bytes < IO_CHUNK_BYTES as u64 {
            chunks
                .iter()
                .map(|c| (c.end - c.start) as usize)
                .max()
                .unwrap_or(0)
        } else {
            chunk_bytes
        };
        let source = Arc::new(Source {
            files,
            direct,
            chunks,
            chunk_bytes,
            readers,
            bufs,
            buffer_cap: buffer_bytes + BUFFER_SLACK,
            stats: Stats::new(start),
        });
        let stream = ChunkStream::start(&source, threads, !odirect);
        par::diagnose(
            threads,
            None,
            if odirect {
                0
            } else {
                readers.min(source.files.len())
            },
        );
        Self {
            source,
            link_chunks,
            stream: Some(stream),
            drop_threads: threads,
        }
    }

    pub(super) fn files(&self) -> &[FileInfo] {
        &self.source.files
    }

    pub(super) fn link_chunks(&self) -> &[Chunk] {
        &self.link_chunks
    }

    pub(super) fn take_stream(&mut self) -> ChunkStream {
        self.stream.take().expect("fast ingest stream")
    }

    pub(super) fn restart(&mut self) {
        drop(self.stream.take());
        self.stream = Some(ChunkStream::start(&self.source, self.drop_threads, false));
    }
}

impl Drop for Input {
    fn drop(&mut self) {
        drop(self.stream.take());
        let Source {
            readers,
            bufs,
            chunk_bytes,
            stats,
            ..
        } = &*self.source;
        let mode = if stats.direct_read.load(Relaxed) {
            "odirect"
        } else {
            "buffered"
        };
        let first = stats.first_byte.load(Relaxed);
        let first = if first == u64::MAX {
            "na".into()
        } else {
            format!("{:.3}", first as f64 * 1e-6)
        };
        eprintln!(
            "[io] mode={mode} readers={readers} bufs={bufs} chunk_kb={} first_byte_ms={first} read_blocked_s={:.6} parse_wait_s={:.6} bytes={}",
            chunk_bytes >> 10,
            stats.read_ns.load(Relaxed) as f64 * 1e-9,
            stats.parse_wait_ns.load(Relaxed) as f64 * 1e-9,
            stats.bytes.load(Relaxed),
        );
    }
}

struct Ready {
    chunks: VecDeque<FilledChunk>,
    active_readers: usize,
    error: Option<io::Error>,
}

struct Shared {
    source: Arc<Source>,
    next: AtomicUsize,
    stop: AtomicBool,
    free: Mutex<Vec<AlignedBuf>>,
    free_cv: Condvar,
    ready: Mutex<Ready>,
    ready_cv: Condvar,
}

impl Shared {
    fn take_free(&self) -> Option<AlignedBuf> {
        let mut free = self.free.lock().unwrap();
        while !self.stop.load(Relaxed) {
            if let Some(buf) = free.pop() {
                return Some(buf);
            }
            free = self.free_cv.wait(free).unwrap();
        }
        None
    }

    fn recycle(&self, buf: AlignedBuf) {
        self.free.lock().unwrap().push(buf);
        self.free_cv.notify_one();
    }

    fn request_stop(&self) {
        {
            let _free = self.free.lock().unwrap();
            if self.stop.swap(true, Relaxed) {
                return;
            }
            self.free_cv.notify_all();
        }
        let _ready = self.ready.lock().unwrap();
        self.ready_cv.notify_all();
    }

    fn read_chunks(&self) {
        while !self.stop.load(Relaxed) {
            let chunk_idx = self.next.fetch_add(1, Relaxed);
            let Some(&chunk) = self.source.chunks.get(chunk_idx) else {
                break;
            };
            let Some(mut buf) = self.take_free() else {
                break;
            };
            let index = chunk.file as usize;
            let file = &self.source.files[index];
            let direct = self.source.direct[index].as_ref();
            match read_chunk_aligned(file, direct, chunk, &mut buf, &self.source.stats) {
                Ok((lo, hi)) => {
                    self.ready.lock().unwrap().chunks.push_back(FilledChunk {
                        chunk_idx,
                        buf,
                        lo,
                        hi,
                    });
                    self.ready_cv.notify_one();
                }
                Err(err) => {
                    self.recycle(buf);
                    self.ready.lock().unwrap().error.get_or_insert(err);
                    self.request_stop();
                    break;
                }
            }
        }

        let mut ready = self.ready.lock().unwrap();
        ready.active_readers -= 1;
        if ready.active_readers == 0 {
            self.ready_cv.notify_all();
        }
    }
}

pub(super) struct ChunkStream {
    shared: Arc<Shared>,
    workers: Vec<JoinHandle<()>>,
    drop_threads: usize,
}

impl ChunkStream {
    fn start(source: &Arc<Source>, drop_threads: usize, advise: bool) -> Self {
        let free = (0..source.bufs)
            .map(|_| AlignedBuf::new(source.buffer_cap).expect("map CSV input buffer"))
            .collect();
        let shared = Arc::new(Shared {
            source: source.clone(),
            next: AtomicUsize::new(0),
            stop: AtomicBool::new(false),
            free: Mutex::new(free),
            free_cv: Condvar::new(),
            ready: Mutex::new(Ready {
                chunks: VecDeque::with_capacity(source.bufs),
                active_readers: source.readers,
                error: None,
            }),
            ready_cv: Condvar::new(),
        });
        // The same workers issue file-priority advice and then read chunks.
        // Give links the advice before the caller starts reading them.
        let startup =
            advise.then(|| Arc::new((AtomicUsize::new(0), Barrier::new(source.readers + 1))));
        let workers = (0..source.readers)
            .map(|_| {
                let shared = shared.clone();
                let startup = startup.clone();
                thread::spawn(move || {
                    if let Some(startup) = startup {
                        loop {
                            let index = startup.0.fetch_add(1, Relaxed);
                            if index >= shared.source.files.len() {
                                break;
                            }
                            shared.source.advise(index);
                        }
                        startup.1.wait();
                    }
                    shared.read_chunks();
                })
            })
            .collect();
        if let Some(startup) = startup {
            if source.readers == 0 {
                for index in 0..source.files.len() {
                    source.advise(index);
                }
            }
            startup.1.wait();
        }
        Self {
            shared,
            workers,
            drop_threads,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.shared.source.chunks.len()
    }

    pub(super) fn kind(&self, index: usize) -> Kind {
        let chunk = self.shared.source.chunks[index];
        self.shared.source.files[chunk.file as usize].kind
    }

    pub(super) fn pop(&self) -> Option<FilledChunk> {
        let mut ready = self.shared.ready.lock().unwrap();
        while !self.shared.stop.load(Relaxed) {
            if let Some(chunk) = ready.chunks.pop_front() {
                return Some(chunk);
            }
            if ready.active_readers == 0 {
                break;
            }
            let start = Instant::now();
            ready = self.shared.ready_cv.wait(ready).unwrap();
            self.shared
                .source
                .stats
                .parse_wait_ns
                .fetch_add(start.elapsed().as_nanos() as u64, Relaxed);
        }
        None
    }

    pub(super) fn recycle(&self, buf: AlignedBuf) {
        self.shared.recycle(buf);
    }

    pub(super) fn request_stop(&self) {
        self.shared.request_stop();
    }

    fn join_and_release(&mut self) -> Option<io::Error> {
        for worker in self.workers.drain(..) {
            worker.join().expect("CSV reader worker");
        }
        let mut bufs = std::mem::take(&mut *self.shared.free.lock().unwrap());
        let mut ready = self.shared.ready.lock().unwrap();
        bufs.extend(ready.chunks.drain(..).map(|chunk| chunk.buf));
        let error = ready.error.take();
        drop(ready);
        debug_assert_eq!(bufs.len(), self.shared.source.bufs);
        if !bufs.is_empty() {
            par::drop_par(self.drop_threads, bufs);
        }
        error
    }

    pub(super) fn finish(mut self) {
        if let Some(err) = self.join_and_release() {
            panic!("read CSV: {err}");
        }
    }

    pub(super) fn cancel(mut self) {
        self.request_stop();
        self.join_and_release();
    }
}

impl Drop for ChunkStream {
    fn drop(&mut self) {
        if !self.workers.is_empty() {
            self.request_stop();
            self.join_and_release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    struct TempCsv(PathBuf);

    impl TempCsv {
        fn new(data: &[u8]) -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let dir = std::env::temp_dir().join(format!(
                "ss-io-test-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Relaxed),
            ));
            fs::create_dir(&dir).unwrap();
            let csv = Self(dir.join("input.csv"));
            fs::write(&csv.0, data).unwrap();
            csv
        }

        fn open(&self) -> FileInfo {
            let handle = File::open(&self.0).unwrap();
            let len = handle.metadata().unwrap().len();
            FileInfo {
                handle,
                len,
                kind: Kind::Events,
            }
        }
    }

    impl Drop for TempCsv {
        fn drop(&mut self) {
            fs::remove_dir_all(self.0.parent().unwrap()).unwrap();
        }
    }

    fn verify_chunks(file: &FileInfo, direct: Option<&DirectFile>, chunk_size: u64) -> Stats {
        let stats = Stats::new(Instant::now());
        let mut buf = AlignedBuf::new(ALIGN).unwrap();
        let mut reference = Vec::new();
        for start in (0..file.len.max(1)).step_by(chunk_size as usize) {
            let end = (start + chunk_size).min(file.len);
            let chunk = Chunk {
                file: 0,
                start,
                end,
            };
            let (lo, hi) = read_chunk_aligned(file, direct, chunk, &mut buf, &stats).unwrap();
            assert_eq!(buf.ptr.as_ptr() as usize % ALIGN, 0);
            let bytes = buf.as_slice(hi + scan::PAD);
            assert_eq!(&bytes[hi..], &[0; scan::PAD]);
            let (lo_ref, hi_ref) = scan::read_chunk(&file.handle, file.len, chunk, &mut reference);
            assert_eq!(
                &bytes[lo..hi],
                &reference[lo_ref..hi_ref],
                "chunk {start}..{end}"
            );
        }
        stats
    }

    #[test]
    fn header_empty_and_one_byte() {
        for data in [
            &b""[..],
            b"x",
            b"\n",
            b"header",
            b"header\n",
            b"header\nrow",
            b"\nrow\n",
            b"headerless first row\nsecond row\n",
        ] {
            let file = TempCsv::new(data).open();
            verify_chunks(&file, None, 64);
            verify_chunks(&file, None, 1);
        }
    }

    #[test]
    fn ownership_at_and_between_newlines() {
        let tmp = TempCsv::new(b"header\nfirst\nsecond\n\nfourth\nlast");
        let file = tmp.open();
        for chunk_size in [1, 3, 7, 13, 23] {
            verify_chunks(&file, None, chunk_size);
        }
    }

    #[test]
    fn long_row_and_header_with_buffer_growth() {
        let long_header = "h".repeat(20_003);
        let long_row = "r".repeat(150_009);
        let tmp = TempCsv::new(format!("{long_header}\nfirst\n{long_row}\n{long_row}").as_bytes());
        let file = tmp.open();
        for chunk_size in [7_321, 64 << 10] {
            verify_chunks(&file, None, chunk_size);
        }
    }

    #[test]
    fn real_direct_file_with_short_eof_and_growth() {
        let long_row = "r".repeat(150_009);
        let tmp = TempCsv::new(format!("header\nshort\n{long_row}\nfinal row").as_bytes());
        let file = tmp.open();
        let direct = DirectFile::open(&tmp.0)
            .unwrap()
            .expect("test requires O_DIRECT filesystem");
        let stats = verify_chunks(&file, Some(&direct), 7_321);
        assert!(stats.direct_read.load(Relaxed));
        assert!(!direct.disabled.load(Relaxed));

        let stats = verify_chunks(&file, Some(&direct), file.len);
        assert_eq!(stats.bytes.load(Relaxed), file.len);
    }

    #[test]
    fn cancel_full_pool_then_read_from_start() {
        let tmp = TempCsv::new(b"header\nrow\n".repeat(10_000).as_slice());
        let file = tmp.open();
        let chunks = scan::plan(std::slice::from_ref(&file), ALIGN as u64);
        let source = Arc::new(Source {
            files: vec![file],
            direct: vec![None],
            chunks,
            chunk_bytes: ALIGN,
            readers: 8,
            bufs: 4,
            buffer_cap: ALIGN + BUFFER_SLACK,
            stats: Stats::new(Instant::now()),
        });

        let stream = ChunkStream::start(&source, 1, true);
        let mut ready = stream.shared.ready.lock().unwrap();
        while ready.chunks.len() < source.bufs {
            ready = stream.shared.ready_cv.wait(ready).unwrap();
        }
        drop(ready);
        stream.cancel();

        let stream = ChunkStream::start(&source, 1, false);
        let mut seen = vec![false; stream.len()];
        while let Some(chunk) = stream.pop() {
            assert!(!std::mem::replace(&mut seen[chunk.chunk_idx], true));
            stream.recycle(chunk.buf);
        }
        stream.finish();
        assert!(seen.into_iter().all(|seen| seen));
    }
}
