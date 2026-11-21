//! Interface between ingest (owner: ingest agent) and aggregate/output.
//! See docs/INTERFACE.md for the contract. Keep stable; announce changes there.

/// Packed byte strings: entry `i` is `bytes[offsets[i]..offsets[i + 1]]`.
#[derive(Default, Clone, Debug)]
pub struct StringTable {
    pub bytes: Vec<u8>,
    /// `len() + 1` entries, starting with 0.
    pub offsets: Vec<u64>,
}

impl StringTable {
    pub fn new() -> Self {
        Self { bytes: Vec::new(), offsets: vec![0] }
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline]
    pub fn get(&self, i: usize) -> &[u8] {
        &self.bytes[self.offsets[i] as usize..self.offsets[i + 1] as usize]
    }
    pub fn push(&mut self, s: &[u8]) {
        if self.offsets.is_empty() {
            self.offsets.push(0);
        }
        self.bytes.extend_from_slice(s);
        self.offsets.push(self.bytes.len() as u64);
    }
}

/// One surviving event (deduplicated, validated, not late). 16 bytes.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Event {
    /// UTC epoch milliseconds, `window_start_ms <= ts_ms < window_end_ms`.
    pub ts_ms: i64,
    /// Index into `Loaded::categories` (sorted byte-wise, so a smaller index is
    /// a byte-wise smaller category name). `unknown` is included.
    pub category: u32,
    /// Purchase price in cents, 1..=10_000_000; 0 means not a purchase.
    pub cents: u32,
}

/// One valid click (deduplicated, known campaign, ts in the click window). 16 bytes.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Click {
    /// UTC epoch milliseconds, `window_start_ms - 7d <= ts_ms < window_end_ms`.
    pub ts_ms: i64,
    /// Index into `Loaded::campaigns` (sorted byte-wise by campaign_id).
    pub campaign: u32,
    /// Reserved (0). Tie order on `ts_ms` is given by position, see `Loaded::clicks`.
    pub reserved: u32,
}

pub struct Loaded {
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    /// Person index -> canonical person id bytes (byte-wise smallest linked id).
    /// Only persons with at least one surviving event are present. Arbitrary order.
    pub person_ids: StringTable,
    /// Category index -> name, sorted ascending byte-wise, unique, includes `unknown`.
    pub categories: Vec<Vec<u8>>,
    /// Campaign index -> (campaign_id, channel), sorted ascending byte-wise by
    /// campaign_id. Never contains `organic` as a real campaign unless the
    /// input file does (it must not, per rules).
    pub campaigns: Vec<(Vec<u8>, Vec<u8>)>,
    /// Surviving events grouped by person (CSR): person `p` owns
    /// `events[event_offsets[p] as usize..event_offsets[p + 1] as usize]`,
    /// sorted by `ts_ms` ascending (ties in arbitrary order). Every person has >= 1 event.
    pub events: Vec<Event>,
    /// `person_ids.len() + 1` entries, `event_offsets[0] == 0`.
    pub event_offsets: Vec<u32>,
    /// Valid clicks grouped by person (CSR, same person indices), sorted by
    /// (`ts_ms` asc, click_id byte-wise asc), deduplicated by click_id. So among
    /// clicks with equal `ts_ms`, the FIRST one has the byte-wise smallest click_id.
    /// Clicks of persons without surviving events are omitted.
    pub clicks: Vec<Click>,
    /// `person_ids.len() + 1` entries, `click_offsets[0] == 0`.
    pub click_offsets: Vec<u32>,
}

impl Loaded {
    #[inline]
    pub fn persons(&self) -> usize {
        self.person_ids.len()
    }
    #[inline]
    pub fn person_events(&self, p: usize) -> &[Event] {
        &self.events[self.event_offsets[p] as usize..self.event_offsets[p + 1] as usize]
    }
    #[inline]
    pub fn person_clicks(&self, p: usize) -> &[Click] {
        &self.clicks[self.click_offsets[p] as usize..self.click_offsets[p + 1] as usize]
    }
}
