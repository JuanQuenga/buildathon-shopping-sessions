//! Minimal deterministic synthetic v5-shaped corpus generator used solely during Docker
//! build-time LLVM PGO profile collection (`INGEST_GEN_PGO=<dir>`). Exercises the exact
//! branch distributions (87.5% short 8/9B + 12.5% 33B hex IDs, 58/10/32% F1/F2/F3 timestamps,
//! 20% quotes, 3% purchases, duplicates, whales, and linked trees/chains) without bundling
//! any dataset files in the repository or runtime image.
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

struct Rng(u64);
impl Rng {
    #[inline]
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    #[inline]
    fn range(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn fmt_id(prefix: u8, n: u64, long_hex: bool, upper: bool) -> String {
    let mut s = String::with_capacity(34);
    s.push(prefix as char);
    if long_hex {
        let a = n.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xA076_1D64_78BD_642F;
        let b = a.wrapping_mul(0xE703_7ED1_A0B4_28DB) ^ 0x8EBC_6AF0_9C88_C6E3;
        if upper {
            s.push_str(&format!("{a:016X}{b:016X}"));
        } else {
            s.push_str(&format!("{a:016x}{b:016x}"));
        }
    } else {
        const B36: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
        let width = if (n & 7) == 1 { 7 } else { 8 };
        let mut v = 100_000_000_000u64 + (n.wrapping_mul(2_654_435_761) % 2_700_000_000_000);
        let mut buf = [0u8; 10];
        let mut k = 0;
        while v > 0 && k < width {
            buf[k] = B36[(v % 36) as usize];
            v /= 36;
            k += 1;
        }
        for b in buf[..k].iter().rev() {
            s.push(*b as char);
        }
    }
    s
}

fn fmt_f1(sec_from_day0: u64) -> String {
    let day = 1 + (sec_from_day0 / 86_400);
    let rem = sec_from_day0 % 86_400;
    let h = rem / 3600;
    let m = (rem % 3600) / 60;
    let s = rem % 60;
    format!("2026-04-{day:02}T{h:02}:{m:02}:{s:02}Z")
}

fn fmt_ts(rng: &mut Rng, sec_from_day0: u64, base_epoch_sec: u64) -> String {
    let r = rng.range(100);
    if r < 58 {
        fmt_f1(sec_from_day0)
    } else if r < 90 {
        format!("{}", (base_epoch_sec + sec_from_day0) * 1000 + rng.range(1000))
    } else {
        let local = sec_from_day0 + 7200;
        let day = 1 + (local / 86_400);
        let rem = local % 86_400;
        format!(
            "2026-04-{day:02}T{:02}:{:02}:{:02}+02:00",
            rem / 3600,
            (rem % 3600) / 60,
            rem % 60
        )
    }
}

#[inline]
fn write_col(w: &mut impl Write, rng: &mut Rng, s: &str) {
    if rng.range(5) == 0 {
        let _ = write!(w, "\"{s}\"");
    } else {
        let _ = w.write_all(s.as_bytes());
    }
}

pub fn generate(dir: &Path) {
    fs::create_dir_all(dir.join("events")).expect("mkdir events");
    fs::create_dir_all(dir.join("clicks")).expect("mkdir clicks");
    fs::create_dir_all(dir.join("links")).expect("mkdir links");

    // 14-day window: 2026-04-08T00:00:00Z .. 2026-04-22T00:00:00Z
    // Day 0 in fmt_f1 is 2026-04-01; window starts at day offset 7 (2026-04-08).
    const BASE_EPOCH_SEC: u64 = 1_774_992_000; // 2026-04-01T00:00:00Z
    const WIN_START_OFF: u64 = 7 * 86_400;
    const WIN_SPAN: u64 = 14 * 86_400;

    fs::write(
        dir.join("manifest.json"),
        "{\"window_start\":\"2026-04-08T00:00:00Z\",\"window_end\":\"2026-04-22T00:00:00Z\"}\n",
    )
    .expect("manifest");

    let mut rng = Rng(0x517C_C1B7_2722_0A95);

    {
        let mut w = BufWriter::new(File::create(dir.join("products.csv")).expect("products"));
        writeln!(w, "product_id,category").unwrap();
        for i in 0..5000 {
            writeln!(w, "p{i},cat_{}", i % 20).unwrap();
        }
    }
    {
        let mut w = BufWriter::new(File::create(dir.join("campaigns.csv")).expect("campaigns"));
        writeln!(w, "campaign_id,channel").unwrap();
        for i in 0..500 {
            writeln!(w, "c_{i},ch_{}", i % 5).unwrap();
        }
    }

    // Build 18,000 persons with 1..4 IDs each (1/8 long 33B hex) -> ~32,000 IDs, ~18,000 links.
    const NPERSONS: usize = 18_000;
    let mut person_ids: Vec<Vec<String>> = Vec::with_capacity(NPERSONS);
    let mut uid_seq = 1u64;
    let mut lw = BufWriter::new(File::create(dir.join("links/part-0.csv")).expect("links"));
    writeln!(lw, "user_id_a,user_id_b").unwrap();
    for p in 0..NPERSONS {
        let k = if p < 5 {
            4
        } else {
            match rng.range(100) {
                0..=44 => 1,
                45..=74 => 2,
                75..=89 => 3,
                _ => 4,
            }
        };
        let mut ids = Vec::with_capacity(k);
        for _ in 0..k {
            let is_long = (uid_seq & 7) == 0;
            ids.push(fmt_id(b'u', uid_seq, is_long, false));
            uid_seq += 1;
        }
        for i in 1..ids.len() {
            write_col(&mut lw, &mut rng, &ids[i - 1]);
            lw.write_all(b",").unwrap();
            write_col(&mut lw, &mut rng, &ids[i]);
            lw.write_all(b"\n").unwrap();
        }
        person_ids.push(ids);
    }
    lw.flush().unwrap();

    // Generate 450,000 events across 4 part files (including whales, duplicates, late/bad rows).
    let mut eid_seq = 1u64;
    let mut dup_buf: Vec<(String, String, u64)> = Vec::with_capacity(20_000);
    for part in 0..4usize {
        let mut ew =
            BufWriter::new(File::create(dir.join(format!("events/part-{part}.csv"))).expect("ev"));
        writeln!(ew, "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents").unwrap();
        for _ in 0..112_500 {
            let p = if rng.range(100) < 6 {
                rng.range(5) as usize // whales -> DenseSessions + FusedAssists Day-Run
            } else {
                rng.range(NPERSONS as u64) as usize
            };
            let ids = &person_ids[p];
            let uid = &ids[rng.range(ids.len() as u64) as usize];
            let eid = fmt_id(b'e', eid_seq, (eid_seq & 7) == 0, (eid_seq & 4095) == 0);
            eid_seq += 1;

            let ts_off = WIN_START_OFF + rng.range(WIN_SPAN);
            let ing_delay = if rng.range(100) == 0 { 90_000 } else { rng.range(600) };
            let ing_off = ts_off + ing_delay;

            if rng.range(33) == 0 && dup_buf.len() < 15_000 {
                dup_buf.push((eid.clone(), uid.clone(), ts_off));
            }

            let t_str = fmt_ts(&mut rng, ts_off, BASE_EPOCH_SEC);
            let i_str = fmt_f1(ing_off);
            let ty_r = rng.range(100);
            let (ty, price) = if ty_r < 75 {
                ("view", String::new())
            } else if ty_r < 97 {
                ("cart", String::new())
            } else {
                ("purchase", format!("{}", 100 + rng.range(49_900)))
            };
            let prod = format!("p{}", rng.range(5000));

            write_col(&mut ew, &mut rng, &eid);
            ew.write_all(b",").unwrap();
            write_col(&mut ew, &mut rng, uid);
            ew.write_all(b",").unwrap();
            write_col(&mut ew, &mut rng, &t_str);
            ew.write_all(b",").unwrap();
            write_col(&mut ew, &mut rng, &i_str);
            ew.write_all(b",").unwrap();
            write_col(&mut ew, &mut rng, ty);
            ew.write_all(b",").unwrap();
            write_col(&mut ew, &mut rng, &prod);
            ew.write_all(b",").unwrap();
            write_col(&mut ew, &mut rng, &price);
            ew.write_all(b"\n").unwrap();
        }
        if part == 3 {
            for (eid, uid, ts_off) in &dup_buf {
                let t_str = fmt_ts(&mut rng, *ts_off, BASE_EPOCH_SEC);
                let i_str = fmt_f1(*ts_off + 1200);
                writeln!(ew, "{eid},{uid},{t_str},{i_str},view,p1,").unwrap();
            }
        }
        ew.flush().unwrap();
    }

    // Generate 110,000 clicks across 2 part files.
    let mut cid_seq = 1u64;
    for part in 0..2usize {
        let mut cw =
            BufWriter::new(File::create(dir.join(format!("clicks/part-{part}.csv"))).expect("ck"));
        writeln!(cw, "click_id,user_id,campaign_id,ts").unwrap();
        for _ in 0..55_000 {
            let p = if rng.range(100) < 6 {
                rng.range(5) as usize
            } else {
                rng.range(NPERSONS as u64) as usize
            };
            let ids = &person_ids[p];
            let uid = &ids[rng.range(ids.len() as u64) as usize];
            let cid = fmt_id(b'k', cid_seq, (cid_seq & 7) == 0, false);
            cid_seq += 1;
            let camp = if rng.range(10) < 3 {
                format!("c_{}", rng.range(5))
            } else {
                format!("c_{}", rng.range(500))
            };
            let ts_off = rng.range(WIN_START_OFF + WIN_SPAN);
            let t_str = fmt_ts(&mut rng, ts_off, BASE_EPOCH_SEC);
            write_col(&mut cw, &mut rng, &cid);
            cw.write_all(b",").unwrap();
            write_col(&mut cw, &mut rng, uid);
            cw.write_all(b",").unwrap();
            write_col(&mut cw, &mut rng, &camp);
            cw.write_all(b",").unwrap();
            write_col(&mut cw, &mut rng, &t_str);
            cw.write_all(b"\n").unwrap();
        }
        cw.flush().unwrap();
    }
}
