#[path = "../output/mod.rs"]
pub mod output;

use crate::proto_c::arena::Region;

pub const DAY_MS: i64 = 86_400_000;
pub const NO_CATEGORY: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CampCell {
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assisted: i64,
}

/// One campaign_daily row. `campaign == campaigns.len()` means `organic`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CampRow {
    pub campaign: u32,
    pub day: i32,
    pub cell: CampCell,
}

#[inline]
pub fn day_of(ts: i64) -> i32 {
    ts.div_euclid(DAY_MS) as i32
}

/// Per-worker campaign x day cells in pages recycled from deduplication.
pub struct CampStore<'a> {
    day0: i32,
    ndays: usize,
    dense: &'a mut [CampCell],
}

impl<'a> CampStore<'a> {
    pub fn new(region: &'a Region<'_>, ncamp: usize, day0: i32, ndays: usize) -> Option<Self> {
        let dense = region.slice(ncamp.checked_mul(ndays)?, CampCell::default())?;
        Some(Self { day0, ndays, dense })
    }

    #[inline(always)]
    pub fn row(&mut self, camp: u32) -> &mut [CampCell] {
        let start = camp as usize * self.ndays;
        &mut self.dense[start..start + self.ndays]
    }

    /// # Safety
    /// The campaign and relative day must be within this table.
    #[inline(always)]
    pub unsafe fn cell_in_window(&mut self, campaign: u32, day: usize) -> &mut CampCell {
        debug_assert!(day < self.ndays);
        unsafe {
            self.dense
                .get_unchecked_mut(campaign as usize * self.ndays + day)
        }
    }
}

pub fn merge_camps<'a>(stores: impl IntoIterator<Item = CampStore<'a>>) -> Vec<CampRow> {
    let mut stores = stores.into_iter();
    let Some(merged) = stores.next() else {
        return Vec::new();
    };
    for store in stores {
        for (d, s) in merged.dense.iter_mut().zip(store.dense) {
            d.sessions += s.sessions;
            d.purchasing += s.purchasing;
            d.revenue += s.revenue;
            d.assisted += s.assisted;
        }
    }
    merged
        .dense
        .iter()
        .enumerate()
        .filter_map(|(i, cell)| {
            (cell.sessions != 0 || cell.assisted != 0).then(|| CampRow {
                campaign: (i / merged.ndays) as u32,
                day: merged.day0 + (i % merged.ndays) as i32,
                cell: *cell,
            })
        })
        .collect()
}
