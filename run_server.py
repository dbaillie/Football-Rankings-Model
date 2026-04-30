"""Serve the dashboard using webapp.backend.main:app (avoids launching the wrong module)."""

from __future__ import annotations

import os

import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("FOOTBALL_RANKINGS_PORT", os.environ.get("PORT", "8000")))
    print(
        "\n"
        "================================================================================\n"
        f"  Football Rankings — http://127.0.0.1:{port}\n"
        "  Open that URL (same port) so /api/* matches this process.\n"
        "\n"
        "  If http://127.0.0.1:8000/api/ping-club returns {\"detail\":\"Not Found\"} but\n"
        "  this terminal shows probe routes including /api/ping-club, port 8000 is owned\n"
        "  by another program. Use a free port, e.g. in PowerShell:\n"
        "\n"
        "    $env:FOOTBALL_RANKINGS_PORT=8010; python run_server.py\n"
        "\n"
        "  Then open http://127.0.0.1:8010/api/ping-club\n"
        "================================================================================\n",
        flush=True,
    )
    uvicorn.run(
        "webapp.backend.main:app",
        host="127.0.0.1",
        port=port,
        reload=True,
    )
