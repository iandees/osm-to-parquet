// bbox: a Minneapolis restaurant cluster near (44.948,-93.253), hardcoded --
// a small cluster of restaurants around (44.948,-93.253) with several
// amenity nodes created and two modified between 19:34 and 19:51 UTC on
// 2026-09-19 (well after the history base and after the retro date below),
// so this retro snapshot is guaranteed to differ from the current state:
// fewer nodes, and older tags/versions on the two that were edited.
//
// NB: an earlier draft of this entry tried to express "a difference
// against now" literally, with `a->.now; retro(t){ a->.then; } (.now; -
// .then;)`. Probing the reference directly showed that a named set
// assigned *inside* a retro() block is empty once read *outside* it (my
// probe: `retro("...") { node(...)->.then; } .then; out count;` returned
// count=0) -- retro()'s set-scoping does not match docs/m4-contracts.md
// section 3.2's "sets assigned inside remain visible outside" claim, at
// least for this reference version. That pattern silently degenerates to
// printing the *unfiltered* "now" set (their difference is bugged, or
// retro sets never leak), which is not a meaningful test of retro. This
// simpler form -- a plain retro() snapshot at a date proven to differ
// from now -- is the real test of retro's time-travel.
[out:json][timeout:25];
retro("2026-09-19T06:00:00Z"){ node(44.9470,-93.2545,44.9490,-93.2520)[amenity]; out meta; }
