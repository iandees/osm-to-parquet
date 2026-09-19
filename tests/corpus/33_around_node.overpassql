// Stadium Village light-rail station (node 2913631282, Minneapolis) and
// cafes within 300m of it -- verified at the reference server to return
// exactly one match, "Mu Mu Tea" (node 6332252021). Static id lookup: no
// bbox placeholder needed, the (around:) filter's own implied bbox scopes
// the candidate scan.
[out:json][timeout:25];
node(id:2913631282)->.station;
node[amenity=cafe](around.station:300);
out;
