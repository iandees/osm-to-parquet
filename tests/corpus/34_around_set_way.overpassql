// Franklin Avenue light-rail station (node 1819182328, Minneapolis) and
// its railway platform way within 60m of it -- verified at the reference
// server to return exactly one match, way 99713933. Static id lookup: no
// bbox placeholder needed.
[out:json][timeout:25];
node(id:1819182328)->.a;
way[railway=platform](around.a:60);
out ids;
