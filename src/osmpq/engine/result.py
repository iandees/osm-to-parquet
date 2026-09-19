"""`osmpq.engine.result.Result`: the value `Engine.run()` returns.

`.render()` produces the JSON or XML text exactly in the envelope shapes of
docs/m0-contracts.md sections 6-7. Both renderers are pure formatting over
the same `elements` list of Overpass-JSON-shaped dicts (see
`osmpq.engine.render.build_elements`).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional
from xml.sax.saxutils import escape as xml_escape
from xml.sax.saxutils import quoteattr

from osmpq.ql.ast import Settings

GENERATOR = "osmpq 0.0.1"
COPYRIGHT = (
    "The data included in this document is from www.openstreetmap.org. "
    "The data is made available under ODbL."
)

_CSV_OTYPE = {"node": "n", "way": "w", "relation": "r"}


def _csv_header_name(field: str) -> str:
    """The reference server's CSV header spells a special field "@id" (not
    "::id"), confirmed empirically (`[out:csv(name,::id,::lat,::lon)]`
    against maps.mail.ru/.../interpreter, see
    tests/corpus/47_out_csv.overpassql); a plain tag key keeps its own
    name unchanged."""
    if field.startswith("::"):
        return "@" + field[2:]
    return field


def _csv_lat_lon(el: dict, which: str) -> str:
    if el.get("type") == "node":
        v = el.get(which)
    else:
        v = (el.get("center") or {}).get(which)
    return "" if v is None else str(v)


def _csv_field_value(el: dict, field: str) -> str:
    """Contract 3.6: tag keys plus `::id`, `::type`, `::otype`, `::lat`,
    `::lon`, `::count`, `::version`, `::timestamp`, `::changeset`, `::uid`,
    `::user`. `out count` rows fill only `::count` and leave everything
    else empty. Values are written raw, no quoting (contract; matches the
    reference)."""
    if el.get("type") == "count":
        if field == "::count":
            return str((el.get("tags") or {}).get("total", ""))
        return ""
    if field == "::id":
        return str(el.get("id", ""))
    if field == "::type":
        return str(el.get("type", ""))
    if field == "::otype":
        return _CSV_OTYPE.get(el.get("type"), "")
    if field == "::lat":
        return _csv_lat_lon(el, "lat")
    if field == "::lon":
        return _csv_lat_lon(el, "lon")
    if field == "::count":
        return ""
    if field in ("::version", "::timestamp", "::changeset", "::uid", "::user"):
        v = el.get(field[2:])
        return "" if v is None else str(v)
    return (el.get("tags") or {}).get(field, "")


@dataclass
class Result:
    elements: list[dict] = field(default_factory=list)
    settings: Optional[Settings] = None
    remark: Optional[str] = None
    timestamp_osm_base: Optional[str] = None
    stats: dict = field(default_factory=dict)
    # docs/m4-contracts.md section 3.2: `[diff:]`/`[adiff:]` -- `elements`
    # then holds action dicts (`{"action", "type", "id", "old"?, "new"?}`)
    # instead of plain Overpass elements. JSON renders them as-is (our
    # documented extension over the reference, which errors instead); XML
    # gets its own `<action type="...">` wrapper via `_action_xml`.
    is_diff: bool = False

    def render(self) -> tuple[str, str]:
        fmt = self.settings.out_format if self.settings is not None else "json"
        if fmt == "xml":
            return self._render_xml(), "application/osm3s+xml"
        if fmt == "csv":
            return self._render_csv(), "text/csv; charset=utf-8"
        return self._render_json(), "application/json"

    # ---------------------------------------------------------------- JSON

    def _render_json(self) -> str:
        envelope: dict = {
            "version": 0.6,
            "generator": GENERATOR,
            "osm3s": {
                "timestamp_osm_base": self.timestamp_osm_base,
                "copyright": COPYRIGHT,
            },
            "elements": self.elements,
        }
        if self.remark:
            envelope["remark"] = self.remark
        return json.dumps(envelope, indent=2)

    # ----------------------------------------------------------------- CSV

    def _render_csv(self) -> str:
        """Contract 3.6. `settings.csv_fields` is always populated by the
        parser when `out_format == "csv"` (the `[out:csv(...)]` grammar
        requires the parenthesized field list), but this degrades to an
        empty column set rather than erroring if it's ever missing."""
        fields = (self.settings.csv_fields if self.settings else None) or []
        sep = self.settings.csv_separator if self.settings else "\t"
        header = self.settings.csv_header if self.settings else True

        lines: list[str] = []
        if header:
            # Verified against the reference server: a field written
            # "::id" in the query is headed "@id" in the csv output (not
            # "::id") -- the "::" -> "@" spelling is display-only, it does
            # not change which column the field selects.
            lines.append(sep.join(_csv_header_name(f) for f in fields))
        for el in self.elements:
            lines.append(sep.join(_csv_field_value(el, f) for f in fields))
        return "\n".join(lines) + ("\n" if lines else "")

    # ----------------------------------------------------------------- XML

    def _render_xml(self) -> str:
        parts = ['<?xml version="1.0" encoding="UTF-8"?>']
        parts.append(f'<osm version="0.6" generator="{xml_escape(GENERATOR)}">')
        parts.append(f"<note>{xml_escape(COPYRIGHT)}</note>")
        if self.timestamp_osm_base:
            parts.append(f'<meta osm_base="{xml_escape(self.timestamp_osm_base)}"/>')
        for el in self.elements:
            parts.append(_action_xml(el) if self.is_diff else _element_xml(el))
        if self.remark:
            parts.append(f"<remark>{xml_escape(self.remark)}</remark>")
        parts.append("</osm>")
        return "".join(parts)


def _attr(name: str, value) -> str:
    if value is None:
        return ""
    return f" {name}={quoteattr(str(value))}"


def _meta_attrs(el: dict) -> str:
    out = ""
    out += _attr("version", el.get("version"))
    out += _attr("timestamp", el.get("timestamp"))
    out += _attr("changeset", el.get("changeset"))
    out += _attr("uid", el.get("uid"))
    out += _attr("user", el.get("user"))
    # docs/m4-contracts.md section 3.2: an `adiff` raw old/new stub carries
    # an explicit `visible` flag (probe `adiff_xml`: `visible="true"` for
    # an element that still exists but fell out of the query, `"false"`
    # for a real OSM deletion); absent on every ordinary element.
    if "visible" in el:
        out += _attr("visible", "true" if el["visible"] else "false")
    return out


def _tags_xml(el: dict) -> str:
    tags = el.get("tags")
    if not tags:
        return ""
    return "".join(f"<tag k={quoteattr(k)} v={quoteattr(v)}/>" for k, v in tags.items())


def _bounds_xml(el: dict) -> str:
    b = el.get("bounds")
    if not b:
        return ""
    return (
        f'<bounds minlat="{b["minlat"]}" minlon="{b["minlon"]}" '
        f'maxlat="{b["maxlat"]}" maxlon="{b["maxlon"]}"/>'
    )


def _center_xml(el: dict) -> str:
    c = el.get("center")
    if not c:
        return ""
    return f'<center lat="{c["lat"]}" lon="{c["lon"]}"/>'


def _element_xml(el: dict) -> str:
    t = el["type"]
    if t == "count":
        tags = el.get("tags") or {}
        body = "".join(f'<tag k={quoteattr(k)} v={quoteattr(v)}/>' for k, v in tags.items())
        return f"<count>{body}</count>"
    if t == "node":
        return _node_xml(el)
    if t == "way":
        return _way_xml(el)
    if t == "relation":
        return _relation_xml(el)
    if t == "area":
        return _area_xml(el)
    if t == "timeline":
        return _timeline_xml(el)
    return ""


def _timeline_xml(el: dict) -> str:
    # docs/m4-contracts.md section 3.2: `<timeline id=".."><tag .../></timeline>`
    # (probe `timeline_xml.xml`), same `<tag>` shape as any other element.
    return f'<timeline id="{el["id"]}">' + _tags_xml(el) + "</timeline>"


def _action_xml(action: dict) -> str:
    """`[diff:]`/`[adiff:]` XML (docs/m4-contracts.md section 3.2, probes
    `diff_xml`/`adiff_xml`): `<action type="create">` wraps the element
    directly, `"modify"` wraps `<old>`/`<new>`, `"delete"` wraps `<old>`
    and -- for `adiff`, when the object's raw state is still known at `b`
    -- a `<new>` stub (`_meta_attrs`' `visible` attribute distinguishes a
    real OSM deletion from one that merely fell out of the query)."""
    kind = action["action"]
    body = ""
    if kind == "create":
        body = _element_xml(action["new"])
    elif kind == "delete":
        body = "<old>\n  " + _element_xml(action["old"]) + "\n</old>"
        new = action.get("new")
        if new is not None:
            body += "\n<new>\n  " + _element_xml(new) + "\n</new>"
    else:  # modify
        body = (
            "<old>\n  " + _element_xml(action["old"]) + "\n</old>\n"
            "<new>\n  " + _element_xml(action["new"]) + "\n</new>"
        )
    return f'<action type="{kind}">\n{body}\n</action>\n'


def _node_xml(el: dict) -> str:
    head = f'<node id="{el["id"]}"'
    head += _attr("lat", el.get("lat"))
    head += _attr("lon", el.get("lon"))
    head += _meta_attrs(el)
    tags = _tags_xml(el)
    if not tags:
        return head + "/>"
    return head + ">" + tags + "</node>"


def _way_xml(el: dict) -> str:
    head = f'<way id="{el["id"]}"' + _meta_attrs(el)
    body = _bounds_xml(el) + _center_xml(el)
    nodes = el.get("nodes")
    geometry = el.get("geometry")
    if nodes is not None and geometry is not None and len(nodes) == len(geometry):
        for ref, pt in zip(nodes, geometry):
            # out geom(bbox): a vertex outside the clip bbox (and not
            # adjacent to one inside it) has pt=None -- render.py's module
            # docstring -- a bare <nd ref=.../> with no lat/lon, exactly
            # like the reference.
            if pt is None:
                body += f'<nd ref="{ref}"/>'
            else:
                body += f'<nd ref="{ref}" lat="{pt["lat"]}" lon="{pt["lon"]}"/>'
    elif nodes is not None:
        for ref in nodes:
            body += f'<nd ref="{ref}"/>'
    elif geometry is not None:
        for pt in geometry:
            body += f'<nd lat="{pt["lat"]}" lon="{pt["lon"]}"/>'
    body += _tags_xml(el)
    if not body:
        return head + "/>"
    return head + ">" + body + "</way>"


def _area_xml(el: dict) -> str:
    # docs/m3-contracts.md section 4.4: an `<area>` element has no geometry
    # of its own -- just its id, optional meta attributes and tags.
    head = f'<area id="{el["id"]}"' + _meta_attrs(el)
    tags = _tags_xml(el)
    if not tags:
        return head + "/>"
    return head + ">" + tags + "</area>"


def _relation_xml(el: dict) -> str:
    head = f'<relation id="{el["id"]}"' + _meta_attrs(el)
    body = _bounds_xml(el) + _center_xml(el)
    for m in el.get("members") or []:
        m_head = f'<member type="{m["type"]}" ref="{m["ref"]}" role={quoteattr(m.get("role") or "")}'
        if m["type"] == "node" and "lat" in m:
            body += m_head + f' lat="{m["lat"]}" lon="{m["lon"]}"/>'
        elif m["type"] == "way" and "geometry" in m:
            nds = "".join(f'<nd lat="{p["lat"]}" lon="{p["lon"]}"/>' for p in m["geometry"])
            body += m_head + ">" + nds + "</member>"
        else:
            body += m_head + "/>"
    body += _tags_xml(el)
    if not body:
        return head + "/>"
    return head + ">" + body + "</relation>"
