//! Bytes on disk -> `Loaded`. Owner: ingest agent.
use crate::types::*;
use std::path::Path;

pub fn load(_data_dir: &Path, _threads: usize) -> Loaded {
    Loaded {
        window_start_ms: 0,
        window_end_ms: 0,
        person_ids: StringTable::new(),
        categories: vec![b"unknown".to_vec()],
        campaigns: Vec::new(),
        events: Vec::new(),
        event_offsets: vec![0],
        clicks: Vec::new(),
        click_offsets: vec![0],
    }
}
