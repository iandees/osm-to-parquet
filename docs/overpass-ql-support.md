# Overpass QL support matrix

The reference is the [Overpass QL wiki page](https://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL)
and the behavior of `overpass-api.de`. Tiers are the order we intend to
implement features in; "how" points at the mechanism in `design.md`.

Compatibility target: **most real-world queries produce the same set of
elements with the same tags and geometry.** Byte-identical output, exact
ordering under `qt`, and exotic evaluator corner cases are explicitly not
goals; differences are documented rather than chased.

Legend: T1 = milestone 0/1 (must have for overpass turbo and JOSM to work),
T2 = milestone 3, T3 = milestone 5, T4 = attic (milestone 4), N = not planned.

## Settings

| Feature | Tier | Notes |
| --- | --- | --- |
| `[out:json]`, `[out:xml]` | T1 | Same envelope shape incl. `osm3s.timestamp_osm_base`, `remark` |
| `[out:csv(...)]` | T2 | Column list, header flag, separator |
| `[out:popup]`, `[out:custom]` | T3 | Rarely used |
| `[timeout:n]` | T1 | Cancels the DuckDB query; Overpass-style runtime error remark |
| `[maxsize:n]` | T1 | Maps to DuckDB memory limit for the query |
| `[bbox:s,w,n,e]` | T1 | Global bbox applied to every query statement |
| `[date:"..."]` | T4 | History table, `valid_from <= t < valid_to` |
| `[diff:"a","b"]`, `[adiff:"a","b"]` | T4 | Attic |

## Query statements and filters

| Feature | Tier | Notes |
| --- | --- | --- |
| `node`, `way`, `rel`/`relation`, `nwr`, `nw`, `nr`, `wr` | T1 | |
| `area` (as a query type) | T2 | Reads the derived `area` table |
| `derived` | T3 | Only meaningful with `make`/`convert` |
| Tag filters `[k=v]`, `[k!=v]`, `[k]`, `[!k]`, `[k~v]`, `[k!~v]`, `[~k~v]`, `,i` flag | T1 | Promoted columns get pushdown; the rest evaluate on the `tags` map |
| Bounding box `(s,w,n,e)` | T1 | Cell selection + row-group pruning |
| `(id:...)`, `(n)` single id | T1 | Via id-to-cell index |
| Input set `.name` on a query, `->.name` assignment | T1 | Temp tables |
| Recurse filters `(w)`, `(r)`, `(bn)`, `(bw)`, `(br)`, with role `(r:"role")` etc. | T1 | Same machinery as `>`/`<` |
| `(around:r)`, `(around.set:r)`, `(around:r,lat,lon,...)` | T2 | Bbox pre-filter + spheroid distance |
| `(poly:"lat lon ...")` | T2 | |
| `(area)`, `(area.set)`, `(area:id)` | T2 | Point/geometry in polygon against `area` |
| `(pivot)`, `(pivot.set)` | T2 | |
| `(newer:"ts")`, `(changed:"a")`, `(changed:"a","b")` | T2 | Meta columns; `changed` with a range needs history (T4) for exactness, T2 approximates with "last edit in range" like Overpass without attic |
| `(user:"name")`, `(uid:n)` | T2 | |
| `(if: expr)` | T3 | Evaluator subset compiled to SQL |
| `way_cnt`, `way_link` | T3 | Node-degree filters; needs node→way index or a scan |

## Standalone statements

| Feature | Tier | Notes |
| --- | --- | --- |
| `out` with `ids`, `skel`, `body`, `tags`, `meta`, `noids`, `geom`, `bb`, `center`, `count`, `qt`, `asc`, limit `N` | T1 | `qt` order uses our Hilbert key; a documented difference from Overpass's Z-order quadtiles |
| `out geom(s,w,n,e)` (clipped geometry) | T2 | |
| `.set;` item, `->.set` | T1 | |
| `>` , `>>` , `<` , `<<` | T1 | Core recursion |
| `is_in`, `is_in(lat,lon)` | T2 | |
| `map_to_area` | T2 | Set of ways/relations → area ids |
| `timeline` | T4 | Attic |
| `local` | T3 | Localized geometry representation; rarely used |
| `convert`, `make` | T3 | Requires evaluators |

## Block statements

| Feature | Tier | Notes |
| --- | --- | --- |
| Union `( ... )` | T1 | |
| Difference `( ... - ... )` | T1 | |
| Intersection `.a.b` (multiple input sets on a query) | T1 | |
| `if (expr) { } else { }` | T2 | Needs evaluator subset for the condition |
| `foreach { }`, `foreach.a->.b { }` | T2 | Planner loop |
| `for (expr) { }` | T3 | |
| `complete { }` | T3 | Fixed-point loop |
| `retro (ts) { }`, `compare (delta: ...) { }` | T4 | Attic |

## Evaluators

| Feature | Tier | Notes |
| --- | --- | --- |
| Literals, `t["k"]`, `is_tag("k")`, `id()`, `type()`, `version()`, `timestamp()`, `changeset()`, `uid()`, `user()`, `count_tags()`, `count_members()`, `count_distinct_members()`, `count_by_role()` | T3 | |
| Arithmetic, comparison, boolean, ternary, string ops, `number()`, `is_number()`, `is_date()`, `date()`, `suffix()`, `lrs_*` | T3 | |
| Geometry evaluators: `length()`, `geom()`, `center()`, `trace()`, `hull()`, `lat()`, `lon()` | T3 | Map to spatial functions |
| Aggregators: `count(nodes)` etc, `set(...)`, `min`, `max`, `sum`, `u(...)` | T3 | |
| `keys()`, `per_member`, `per_vertex` | T3/N | Low priority |

## Output formats and details

| Feature | Tier | Notes |
| --- | --- | --- |
| Element ordering (nodes, ways, relations; by id) | T1 | Default order matched; `qt` order differs (see above) |
| `count` element for `out count` | T1 | |
| `remark` on timeout/errors, HTTP 400 with the Overpass HTML error page for parse errors | T1 | overpass turbo parses the error page |
| `bounds` on `out bb`/`geom`, `center` on `out center` | T1 | |
| Coordinates as 7-decimal doubles in JSON, strings in XML | T1 | Match Overpass formatting where cheap; not a hard goal |
| `/api/status`, `/api/timestamp`, `/api/kill_my_queries` | T2 | |
| Overpass XML query language (`<osm-script>`) | N | Could be added as a converter to the same AST later |
| overpass turbo shortcuts (`{{bbox}}`, `{{geocodeArea}}`) | N | Expanded client-side by overpass turbo, not by the server |
