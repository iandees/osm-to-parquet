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


@dataclass
class Result:
    elements: list[dict] = field(default_factory=list)
    settings: Optional[Settings] = None
    remark: Optional[str] = None
    timestamp_osm_base: Optional[str] = None
    stats: dict = field(default_factory=dict)

    def render(self) -> tuple[str, str]:
        fmt = self.settings.out_format if self.settings is not None else "json"
        if fmt == "xml":
            return self._render_xml(), "application/osm3s+xml"
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

    # ----------------------------------------------------------------- XML

    def _render_xml(self) -> str:
        parts = ['<?xml version="1.0" encoding="UTF-8"?>']
        parts.append(f'<osm version="0.6" generator="{xml_escape(GENERATOR)}">')
        parts.append(f"<note>{xml_escape(COPYRIGHT)}</note>")
        if self.timestamp_osm_base:
            parts.append(f'<meta osm_base="{xml_escape(self.timestamp_osm_base)}"/>')
        for el in self.elements:
            parts.append(_element_xml(el))
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
    return ""


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
