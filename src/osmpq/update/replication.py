"""Osmosis-style replication client, docs/m2-contracts.md section 1.

A replication *source* is a directory that publishes ``state.txt`` (the
latest sequence) and, per sequence ``AAABBBCCC``, ``AAA/BBB/CCC.state.txt``
+ ``AAA/BBB/CCC.osc.gz``. Used both for Geofabrik-style regional mirrors
(``download.openstreetmap.fr/replication/...``) and the planet
(``planet.openstreetmap.org/replication/minute``); same layout.

``httpx`` is used for requests, which respects ``HTTPS_PROXY``/``NO_PROXY``
by default (``trust_env=True``, the httpx default) -- nothing special is
needed here to go through the sandbox's proxy.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import httpx

DEFAULT_RETRIES = 5
DEFAULT_BACKOFF = 1.0
DEFAULT_TIMEOUT = 60.0


def sequence_path(seq: int) -> str:
    """``"AAA/BBB/CCC"`` path fragment (no extension) for sequence ``seq``."""
    if seq < 0:
        raise ValueError(f"negative sequence number: {seq}")
    aaa = seq // 1_000_000
    bbb = (seq // 1_000) % 1_000
    ccc = seq % 1_000
    return f"{aaa:03d}/{bbb:03d}/{ccc:03d}"


def parse_state_text(text: str) -> tuple[int, Optional[str]]:
    """Parse an osmosis ``state.txt`` body into ``(sequenceNumber, timestamp)``.

    Java ``Properties`` format: ``#comment`` lines, ``key=value`` lines,
    ``:`` escaped as ``\\:`` in values. ``timestamp`` is returned unescaped
    (e.g. ``"2026-09-19T00:23:06Z"``), or None if the key is absent.
    """
    seq: Optional[int] = None
    ts: Optional[str] = None
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.replace("\\:", ":").replace("\\\\", "\\")
        if key == "sequenceNumber":
            seq = int(value)
        elif key == "timestamp":
            ts = value
    if seq is None:
        raise ValueError(f"no sequenceNumber in state.txt body: {text!r}")
    return seq, ts


@dataclass
class FetchResult:
    seq: int
    osc_path: Path
    state_path: Path
    timestamp: Optional[str]  # from the sequence's own state.txt


class ReplicationClient:
    """Fetches diffs from an osmosis-style replication source, with retries
    and on-disk caching of downloaded ``.osc.gz``/``.state.txt`` under a
    ``--tmpdir``-scoped directory (docs/m2-contracts.md section 5)."""

    def __init__(
        self,
        source: str,
        retries: int = DEFAULT_RETRIES,
        backoff: float = DEFAULT_BACKOFF,
        timeout: float = DEFAULT_TIMEOUT,
        client: Optional[httpx.Client] = None,
    ):
        self.source = source.rstrip("/")
        self.retries = retries
        self.backoff = backoff
        self.timeout = timeout
        self._client = client or httpx.Client(timeout=timeout, follow_redirects=True)
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "ReplicationClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _get(self, url: str) -> Optional[httpx.Response]:
        """GET with retries; None for a 404 ("not yet available" per the
        contract), raises after exhausting retries on other failures."""
        last_exc: Optional[Exception] = None
        for attempt in range(self.retries):
            try:
                r = self._client.get(url)
            except httpx.HTTPError as exc:
                last_exc = exc
                time.sleep(self.backoff * (attempt + 1))
                continue
            if r.status_code == 404:
                return None
            if r.status_code >= 500:
                last_exc = httpx.HTTPStatusError(
                    f"{r.status_code} from {url}", request=r.request, response=r
                )
                time.sleep(self.backoff * (attempt + 1))
                continue
            r.raise_for_status()
            return r
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"GET {url} failed after {self.retries} retries")

    def latest_sequence(self) -> int:
        """The source's current sequence number, from ``<source>/state.txt``."""
        r = self._get(f"{self.source}/state.txt")
        if r is None:
            raise RuntimeError(f"{self.source}/state.txt not found (404)")
        seq, _ts = parse_state_text(r.text)
        return seq

    def fetch(self, seq: int, dest_dir: Path) -> Optional[FetchResult]:
        """Download sequence ``seq``'s ``.osc.gz`` + ``.state.txt`` into
        ``dest_dir``, or reuse them if already cached there. Returns None
        if the sequence is not yet published (404: "not yet available")."""
        dest_dir.mkdir(parents=True, exist_ok=True)
        frag = sequence_path(seq)
        osc_path = dest_dir / f"{seq}.osc.gz"
        state_path = dest_dir / f"{seq}.state.txt"

        timestamp: Optional[str] = None
        if state_path.exists():
            try:
                _seq2, timestamp = parse_state_text(state_path.read_text())
            except Exception:
                state_path.unlink(missing_ok=True)
        if not state_path.exists():
            r = self._get(f"{self.source}/{frag}.state.txt")
            if r is None:
                return None
            state_path.write_text(r.text)
            _seq2, timestamp = parse_state_text(r.text)

        if not osc_path.exists():
            r = self._get(f"{self.source}/{frag}.osc.gz")
            if r is None:
                return None
            tmp = osc_path.with_suffix(".osc.gz.part")
            tmp.write_bytes(r.content)
            tmp.rename(osc_path)

        return FetchResult(seq=seq, osc_path=osc_path, state_path=state_path, timestamp=timestamp)

    def fetch_range(self, from_seq: int, max_diffs: int, dest_dir: Path) -> list[FetchResult]:
        """Fetch consecutive sequences ``from_seq, from_seq+1, ...`` up to
        ``max_diffs`` of them, stopping at the source's current sequence or
        at the first not-yet-available (404) sequence."""
        latest = self.latest_sequence()
        results: list[FetchResult] = []
        seq = from_seq
        while len(results) < max_diffs and seq <= latest:
            r = self.fetch(seq, dest_dir)
            if r is None:
                break
            results.append(r)
            seq += 1
        return results
