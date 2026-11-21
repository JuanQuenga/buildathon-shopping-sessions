# dedup_rules

rules.md Events 1-4: malformed copies are dropped before dedup; dedup is global on event_id keeping the smallest ingest_ts; validation and the late check apply to the kept copy only (an invalid or late earliest copy suppresses the event; the earliest copy's user/ts/price win).
