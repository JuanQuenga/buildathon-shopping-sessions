# id_encoding_edges

rules.md 'IDs compare exactly' / byte-wise smallest: IDs of every length around packing limits (9-11, 20-22, 31-33 chars), prefix pairs, NUL/DEL/control bytes and punctuation must stay distinct and keep byte order for dedup, canonical names, click_id ties and category ties.
