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

## Status after M4

The T4 (attic) rows below are implemented in M4 on a history dataset
that starts at the extract's base timestamp (`docs/m4-report.md`).
Documented differences: `timeline` lists only the versions the history
knows (a regional dataset knows nothing before its base timestamp);
`[date:]` before the history's start answers from the earliest known
state with a `remark`; `(changed:)` cannot be graded against the
reference (it runs out of memory there); areas are not versioned, so
area filters under `[date:]` use the current areas; JSON output for
`[diff:]`/`[adiff:]` is an extension (the reference only renders XML);
`compare` is not implemented.

## Status after M3

Everything marked T1 and T2 below is implemented and graded against a
public Overpass instance on Minnesota (`docs/m3-report.md`). Documented
differences from the reference:

- **Areas.** Closed ways are areas exactly as on the reference (every
  closed way, printed as the way itself); relation areas follow the
  reference's `areas.osm3s` rules. `area[...]` finds closed ways only
  through the keys `name`, `ref`, `admin_level`, `boundary` and `place`
  (an indexed subset; `is_in`, `(area)`, `(pivot)` and `map_to_area` see
  every closed way). Relation areas whose rings leave a regional extract's
  extent (a country, a timezone) do not exist in that extract. Areas
  refresh at compaction, not with every minute's diff.
- **`(changed:"a","b")`** uses the current version's timestamp; without
  attic data, elements edited again after `b`, or whose geometry changed
  through a node edit, differ from the reference.
- **`around`** distances use a local equirectangular projection (within
  0.2% of the spheroid at Minnesota latitudes for the radii people use).
- **`[out:csv]`** matches the reference, including the header spelling
  `::id` as `@id`.
- **`out geom(bbox)`** clips as the reference does: vertices outside the
  bbox keep coordinates only when adjacent to an inside vertex; JSON emits
  `null`, XML a bare `<nd ref>`.
- **`out count`** carries an `areas` tag only when the program used areas,
  as the reference does.
- Evaluators: the T2 subset (5.2 in `docs/m3-contracts.md`); `&&`/`||`
  yield `"1"`/`"0"`, `u()` on a non-unique set yields `""`.

## Settings

| Feature | Tier | Notes |
| --- | --- | --- |
| `[out:json]`, `[out:xml]` | T1 | Same envelope shape incl. `osm3s.timestamp_osm_base`, `remark` |
| `[out:csv(...)]` | T2 | Column list, header flag, separator |
| `[out:popup]`, `[out:custom]` | T3 | Rarely used |
| `[timeout:n]` | T1 | Cancels the DuckDB query; Overpass-style runtime error remark |
| `[maxsize:n]` | T1 | Maps to DuckDB memory limit for the query |
| `[bbox:s,w,n,e]` | T1 | Global bbox applied to every query statement |
| `[date:"..."]` | T4 | Done in M4: state at `t` from the history dataset (`docs/m4-contracts.md` 3.1) |
| `[diff:"a","b"]`, `[adiff:"a","b"]` | T4 | Done in M4: two snapshot passes, XML actions as the reference; JSON is our extension |

## Query statements and filters

| Feature | Tier | Notes |
| --- | --- | --- |
| `node`, `way`, `rel`/`relation`, `nwr`, `nw`, `nr`, `wr` | T1 | |
| `area` (as a query type) | T2 | Relation areas from the derived `area` table; closed ways from the way-area index |
| `derived` | T3 | Only meaningful with `make`/`convert` |
| Tag filters `[k=v]`, `[k!=v]`, `[k]`, `[!k]`, `[k~v]`, `[k!~v]`, `[~k~v]`, `,i` flag | T1 | Promoted columns get pushdown; the rest evaluate on the `tags` map |
| Bounding box `(s,w,n,e)` | T1 | Cell selection + row-group pruning |
| `(id:...)`, `(n)` single id | T1 | Via id-to-cell index |
| Input set `.name` on a query, `->.name` assignment | T1 | Temp tables |
| Recurse filters `(w)`, `(r)`, `(bn)`, `(bw)`, `(br)`, with role `(r:"role")` etc. | T1 | Same machinery as `>`/`<` |
| `(around:r)`, `(around.set:r)`, `(around:r,lat,lon,...)` | T2 | Bbox pre-filter + spheroid distance |
| `(poly:"lat lon ...")` | T2 | |
| `(area)`, `(area.set)`, `(area:id)` | T2 | Node: point in polygon; way: any vertex inside; relation: any member vertex inside |
| `(pivot)`, `(pivot.set)` | T2 | |
| `(newer:"ts")`, `(changed:"a")`, `(changed:"a","b")` | T2/T4 | Exact from the history rows when the dataset has history (M4); meta columns otherwise |
| `(user:"name")`, `(uid:n)` | T2 | |
| `(if: expr)` | T2 | Element-scoped evaluator subset compiled to SQL (done in M3) |
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
| `timeline` | T4 | Done in M4: one entry per own version known to the history |
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
| `retro (ts) { }` | T4 | Done in M4: block-local snapshot, block-local sets (as the reference) |
| `compare (delta: ...) { }` | T4 | Not implemented |

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

## Documented divergences

- **`area` derivation vs. `areas.osm3s`** (docs/m3-contracts.md section
  4.1): an area is derived from every `type=multipolygon`/`type=boundary`
  relation whose member ways assemble into at least one valid ring, and
  from every closed way with `is_area = true` that carries at least one of
  `name`, `ref`, `admin_level`, `boundary`, `place`, `postal_code`,
  `addr:postcode`, `landuse`, `natural`, `leisure`, `amenity`, `tourism`,
  `historic`, `military`, `aeroway`, `water`, `area`. A closed way with
  `is_area = true` but **none** of those keys (overwhelmingly bare
  buildings -- `building=yes` and nothing else, of which the planet has on
  the order of 600 million) gets **no** area of its own; Overpass's own
  `areas.osm3s` recipe applies a similar exclusion for the same reason
  (the area table would otherwise be dominated by uninteresting building
  outlines). A relation needs no such qualifying key: any
  multipolygon/boundary relation with a resolvable ring gets an area
  regardless of its own tags (e.g. a multipolygon relation wrapping a bare
  `building=yes` way still gets an area, even though that way would not on
  its own).
