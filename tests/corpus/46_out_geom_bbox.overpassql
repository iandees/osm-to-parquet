// East Lake Street (Minneapolis) and neighbouring primary-highway ways
// crossing a small clip bbox -- verified at the reference server: way
// 6001527's first node (west of the clip bbox, not adjacent to any node
// inside it) is dropped, its second node (also west of the clip bbox, but
// adjacent to its third node which is inside) keeps its coordinates, and
// way 700231761 (entirely outside the clip bbox) has no `bounds` at all.
// Literal bboxes: no {{bbox}} placeholder needed (two different bboxes
// are used -- the selection bbox and the narrower `out geom()` clip bbox).
[out:json][timeout:25];
way[highway=primary](44.9470,-93.2790,44.9490,-93.2760);
out geom(44.9480,-93.2777,44.9487,-93.2770);
