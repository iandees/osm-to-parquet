# osmpq service image (docs/m3-contracts.md section 6.4).
#
# One image serves both roles, chosen by the command:
#   docker run osmpq                                          # osmpq serve (default CMD)
#   docker run osmpq osmpq updater-server --port 8081          # updater
#
# No Rust toolchain here: this image serves queries and applies
# replication diffs, it does not build planets from a .osm.pbf (that needs
# rust/osmpq-raw, built separately -- see docs/m1-runbook.md).
FROM python:3.12-slim

WORKDIR /app

# pyosmium's compiled extensions link against the system libexpat at
# runtime (unlike its bundled libbz2/liblz4) and python:3.12-slim's
# Debian base doesn't include it.
RUN apt-get update && apt-get install -y --no-install-recommends libexpat1 \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies come from `uv.lock` (uv is the project's package
# manager); only `pyproject.toml`, `uv.lock` and `src` are needed -- see
# .dockerignore for everything else (data/, the Rust target dir, the
# corpus harness cache) that would otherwise bloat the build context.
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock /app/
COPY src /app/src
RUN uv sync --frozen --no-dev --no-editable
ENV PATH="/app/.venv/bin:$PATH"

# Install the `spatial`/`httpfs` DuckDB extensions *now*, into this image's
# HOME (root's -- a small, single-purpose service image, so no dedicated
# user), so `osmpq serve`/`osmpq updater-server` never do a network
# `INSTALL` at request time. Verified in the next step with the network
# made unreachable, so a build that silently depended on a runtime
# download would fail here instead of in production.
ENV HOME=/root
RUN python3 -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL spatial'); con.execute('INSTALL httpfs')"
RUN HTTPS_PROXY=http://127.0.0.1:1 HTTP_PROXY=http://127.0.0.1:1 \
    python3 -c "import duckdb; con = duckdb.connect(); con.execute('LOAD spatial'); con.execute('LOAD httpfs'); print('spatial + httpfs load with no network: OK')"

EXPOSE 8080 8081

CMD ["osmpq", "serve", "--host", "0.0.0.0", "--port", "8080"]
