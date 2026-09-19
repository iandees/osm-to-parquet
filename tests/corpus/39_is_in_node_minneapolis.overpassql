// docs/m3-contracts.md section 4.6: `is_in` on a node. Aster Cafe (same
// node as corpus 30), a node inside the downtown_minneapolis bbox
// (verified at the reference server): no bbox placeholder needed.
[out:json][timeout:25];
node(id:1077152668);
is_in;
out;
