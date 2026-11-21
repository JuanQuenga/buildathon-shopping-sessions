//! Bytes on disk -> `Loaded`. Owner: ingest agent. See docs/ingest.md.
pub mod csv;
pub mod slow;
pub mod small;
pub mod ts;

use crate::types::*;
use std::path::Path;

pub fn load(data_dir: &Path, _threads: usize) -> Loaded {
    slow::load(data_dir)
}
