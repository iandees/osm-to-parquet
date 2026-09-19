// Three cafes near the University of Minnesota East Bank area
// (Minneapolis) -- verified at the reference server to return exactly
// "Starbucks", "Mu Mu Tea" and "Fulton Street Cafe" (order as returned).
// Literal bbox: no {{bbox}} placeholder needed. Graded as an
// order-insensitive multiset of lines after the header (contract 3.6).
[out:csv(name,::id,::lat,::lon)][timeout:25];
node[amenity=cafe](44.965,-93.230,44.980,-93.215);
out;
