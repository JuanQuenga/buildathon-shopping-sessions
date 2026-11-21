#![allow(dead_code)]
pub mod aggregate;
pub mod arena;
pub mod ingest;
pub mod output;
pub mod packed;

use std::path::Path;

pub fn run_to_output(data_dir: &Path, out: &Path, threads: usize) -> Result<arena::Arena, &'static str> {
    ingest::run(data_dir, out, threads)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_ingest_tie_with_differing_payload_triggers_exact_order_fallback() {
        let root = std::env::temp_dir().join(format!("proto-c-tie-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let inp = root.join("in");
        let out = root.join("out");
        std::fs::create_dir_all(inp.join("events")).unwrap();
        std::fs::create_dir_all(inp.join("clicks")).unwrap();
        std::fs::create_dir_all(inp.join("links")).unwrap();
        std::fs::write(
            inp.join("manifest.json"),
            r#"{"window_start":"2026-10-01T00:00:00Z","window_end":"2026-10-15T00:00:00Z"}"#,
        )
        .unwrap();
        std::fs::write(inp.join("products.csv"), "product_id,category\np1,books\n").unwrap();
        std::fs::write(inp.join("campaigns.csv"), "campaign_id,channel\nc1,search\n").unwrap();
        // Differing equal-ingest tie on e1: must return exact-order fallback before writing output
        std::fs::write(
            inp.join("events/part-0.csv"),
            concat!(
                "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n",
                "e1,u1,2026-10-02T00:00:00Z,2026-10-02T00:01:00Z,view,p1,\n",
                "e1,u2,2026-10-02T00:00:00Z,2026-10-02T00:01:00Z,view,p1,\n",
            ),
        )
        .unwrap();
        let err = run_to_output(&inp, &out, 2).err().expect("expected fallback on differing tie");
        assert!(err.starts_with("exact-order:"), "unexpected err: {err}");
        assert!(!out.exists(), "fallback must happen before creating output");

        // Identical equal-ingest duplicate on e1: must succeed on proto-c
        std::fs::write(
            inp.join("events/part-0.csv"),
            concat!(
                "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n",
                "e1,u1,2026-10-02T00:00:00Z,2026-10-02T00:01:00Z,view,p1,\n",
                "e1,u1,2026-10-02T00:00:00Z,2026-10-02T00:01:00Z,view,p1,\n",
            ),
        )
        .unwrap();
        assert!(run_to_output(&inp, &out, 2).is_ok());
        let _ = std::fs::remove_dir_all(&root);
    }
}
