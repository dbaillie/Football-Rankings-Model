const { useEffect, useLayoutEffect, useMemo, useState } = React;

/** Matches dark theme tokens in index.html */
const THEME = {
  text: "#F8FAFC",
  muted: "#94A3B8",
  primary: "#1E3A5F",
  primaryBright: "#60A5FA",
  accent: "#D6A84F",
  success: "#60A5FA",
  geoLand: "#1E293B",
  geoBorder: "#475569",
  geoOcean: "#0B1220",
  plotPaper: "#111827",
  plotGrid: "#1E293B",
};

const UEFA_CODES = new Set(["UCL", "UEL", "UECL", "EURO"]);

/** Lines for country “current top 5 over time” chart (distinct from map heat ramp). */
const COUNTRY_TOP5_LINE_COLORS = ["#60A5FA", "#D6A84F", "#A78BFA", "#2DD4BF", "#FB923C"];

/** Choropleth / markers: weak → strong (all blue — no green in the heat ramp) */
const MAP_HEAT_COLORSCALE = [
  [0, "#1e293b"],
  [0.2, "#1e3a5f"],
  [0.45, "#2563eb"],
  [0.68, "#3b82f6"],
  [0.88, "#60a5fa"],
  [1, "#93c5fd"],
];

function CompetitionBadge({ code }) {
  const raw = String(code ?? "").trim();
  const u = raw.toUpperCase();
  if (UEFA_CODES.has(u)) {
    return <span className="badge badge-uefa">{u}</span>;
  }
  if (/^[A-Z]\d+$/.test(u)) {
    return <span className="badge badge-domestic">{u}</span>;
  }
  if (!raw) {
    return <span className="badge badge-default">—</span>;
  }
  return <span className="badge badge-default">{raw}</span>;
}

function mapHeatValue(p) {
  const best = Number(p.top_team_rating);
  if (Number.isFinite(best)) return best;
  const avg = Number(p.average_rating);
  return Number.isFinite(avg) ? avg : 1500;
}

async function getJson(url, options = {}) {
  const { allow404 = false, timeoutMs = null } = options;
  const ctrl = new AbortController();
  const timer =
    timeoutMs != null && timeoutMs > 0
      ? setTimeout(() => ctrl.abort(), timeoutMs)
      : null;
  try {
    const response = await fetch(url, { signal: ctrl.signal });
    const text = await response.text();
    if (allow404 && response.status === 404) {
      return null;
    }
    if (!response.ok) {
      let detail = text.slice(0, 400);
      try {
        const body = JSON.parse(text);
        if (body && body.detail !== undefined) {
          detail = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail);
        }
      } catch (_) {
        /* ignore */
      }
      throw new Error(`Request failed: ${response.status} (${detail})`);
    }
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch (parseErr) {
      throw new Error(`Invalid JSON (HTTP ${response.status}): ${String(parseErr.message)} — ${text.slice(0, 120)}…`);
    }
  } catch (err) {
    if (err.name === "AbortError") {
      throw new Error(
        `Request timed out after ${Math.round((timeoutMs || 0) / 1000)}s — is the API running and CSV preload finished?`
      );
    }
    throw err;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function fetchClubDetailWithFallbacks(teamId, timeoutMs) {
  const q = encodeURIComponent(teamId);
  const urls = [
    `/api/clubdata?team_id=${q}`,
    `/api/teams/${teamId}/club`,
    `/api/club/${teamId}`,
    `/api/team/${teamId}/club-detail`,
  ];
  const failures = [];
  for (const path of urls) {
    try {
      return await getJson(path, { timeoutMs });
    } catch (e) {
      failures.push(`${path} → ${e.message}`);
    }
  }
  throw new Error(
    `Could not load club data.\n${failures.join("\n")}\n\nQuick checks:\n• GET /api/ping-club (must return JSON — if 404, wrong server or old code)\n• GET /api/clubdata?team_id=${teamId} (query-form club JSON)\n• GET /api/teams/${teamId}/identity\n• GET /api/health\n• From repo root run: python run_server.py`
  );
}

function Plot({ data, layout, config, onClick, onHover, className }) {
  const ref = React.useRef(null);

  useEffect(() => {
    if (!ref.current) return;
    Plotly.newPlot(ref.current, data, layout, {
      responsive: !(layout && layout.width != null && layout.height != null),
      displaylogo: false,
      ...config,
    });
    if (onClick) {
      ref.current.on("plotly_click", onClick);
    }
    if (onHover) {
      ref.current.on("plotly_hover", onHover);
    }
    return () => {
      const node = ref.current;
      if (node) Plotly.purge(node);
    };
  }, [data, layout, config, onClick, onHover]);

  return (
    <div
      ref={ref}
      className={className === "map-plot-host" ? `${className} map-plot-fill` : className}
      style={{
        width: "100%",
        height: className === "map-plot-host" ? "100%" : undefined,
        minHeight: className === "map-plot-host" ? undefined : 420,
      }}
    />
  );
}

const COUNTRY_MAP_COORDS = {
  austria: { lat: 47.6, lon: 14.4, label: "Austria" },
  belgium: { lat: 50.8, lon: 4.4, label: "Belgium" },
  denmark: { lat: 56.2, lon: 10.0, label: "Denmark" },
  england: { lat: 52.8, lon: -1.5, label: "England" },
  finland: { lat: 62.2, lon: 25.7, label: "Finland" },
  france: { lat: 46.2, lon: 2.2, label: "France" },
  germany: { lat: 51.2, lon: 10.4, label: "Germany" },
  greece: { lat: 39.1, lon: 22.9, label: "Greece" },
  ireland: { lat: 53.3, lon: -8.0, label: "Ireland" },
  italy: { lat: 42.8, lon: 12.5, label: "Italy" },
  netherlands: { lat: 52.2, lon: 5.3, label: "Netherlands" },
  norway: { lat: 60.5, lon: 8.4, label: "Norway" },
  poland: { lat: 52.0, lon: 19.1, label: "Poland" },
  portugal: { lat: 39.5, lon: -8.0, label: "Portugal" },
  romania: { lat: 45.9, lon: 24.9, label: "Romania" },
  russia: { lat: 56.5, lon: 37.5, label: "Russia" },
  scotland: { lat: 56.4, lon: -4.2, label: "Scotland" },
  spain: { lat: 40.4, lon: -3.7, label: "Spain" },
  sweden: { lat: 62.0, lon: 15.0, label: "Sweden" },
  switzerland: { lat: 46.8, lon: 8.2, label: "Switzerland" },
  turkey: { lat: 39.0, lon: 35.2, label: "Turkey" },
};

const CHOROPLETH_LOCATION_BY_COUNTRY = {
  austria: "Austria",
  belgium: "Belgium",
  denmark: "Denmark",
  finland: "Finland",
  france: "France",
  germany: "Germany",
  greece: "Greece",
  ireland: "Ireland",
  italy: "Italy",
  netherlands: "Netherlands",
  norway: "Norway",
  poland: "Poland",
  portugal: "Portugal",
  romania: "Romania",
  russia: "Russia",
  spain: "Spain",
  sweden: "Sweden",
  switzerland: "Switzerland",
  turkey: "Turkey",
};

/** Title-case country labels from API slugs (e.g. england → England, bosnia-herzegovina → Bosnia-Herzegovina). */
function formatCountryDisplay(name) {
  if (name == null || name === "") return "";
  return String(name)
    .trim()
    .split(/\s+/)
    .map((segment) =>
      segment
        .split("-")
        .map((part) =>
          part.length === 0 ? part : part.charAt(0).toUpperCase() + part.slice(1).toLowerCase()
        )
        .join("-")
    )
    .join(" ");
}

function formatSignedRating(delta) {
  const n = Number(delta);
  if (!Number.isFinite(n)) return "—";
  const s = n > 0 ? "+" : "";
  return `${s}${n.toFixed(1)}`;
}

/** Pure parse — used with mirrored hash state so routing stays in sync when clicking (not only after full reload). */
function parseHashRouteFromString(hash) {
  const raw = (hash || "#/").replace(/^#/, "");
  const path = raw.startsWith("/") ? raw : `/${raw}`;
  const segments = path.split("/").filter(Boolean);
  if (segments[0] === "country" && segments[1]) {
    return {
      page: "country",
      country: decodeURIComponent(segments[1]).toLowerCase(),
    };
  }
  if (segments[0] === "club" && segments[1]) {
    const rawId = decodeURIComponent(segments[1]);
    const teamId = parseInt(rawId, 10);
    if (Number.isFinite(teamId) && teamId > 0) {
      return { page: "club", teamId: String(teamId) };
    }
  }
  if (segments[0] === "info") {
    return { page: "info" };
  }
  if (segments[0] === "calibration") {
    return { page: "calibration" };
  }
  return { page: "home" };
}

function parseHashRoute() {
  return parseHashRouteFromString(typeof window !== "undefined" ? window.location.hash : "#/");
}

function useHashRoute() {
  const [hash, setHash] = useState(() =>
    typeof window !== "undefined" ? window.location.hash || "#/" : "#/"
  );

  const route = useMemo(() => parseHashRouteFromString(hash), [hash]);

  useEffect(() => {
    const sync = () => setHash(window.location.hash || "#/");
    window.addEventListener("hashchange", sync);
    return () => window.removeEventListener("hashchange", sync);
  }, []);

  const navigate = React.useCallback((path) => {
    const nextHash = path.startsWith("#") ? path : `#${path.startsWith("/") ? path : `/${path}`}`;
    if ((window.location.hash || "#/") !== nextHash) {
      window.location.hash = nextHash;
    }
    setHash(window.location.hash || "#/");
  }, []);

  return { route, navigate, hash };
}

function ContactForm() {
  const [enabled, setEnabled] = useState(null);
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [message, setMessage] = useState("");
  const [company, setCompany] = useState("");
  const [status, setStatus] = useState("idle");
  const [err, setErr] = useState("");

  useEffect(() => {
    let cancel = false;
    getJson("/api/contact/status", { allow404: true })
      .then((d) => {
        if (!cancel) setEnabled(Boolean(d && d.enabled));
      })
      .catch(() => {
        if (!cancel) setEnabled(false);
      });
    return () => {
      cancel = true;
    };
  }, []);

  async function handleSubmit(e) {
    e.preventDefault();
    setErr("");
    setStatus("sending");
    try {
      const res = await fetch("/api/contact", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: name.trim(),
          email: email.trim(),
          message: message.trim(),
          company,
        }),
      });
      const text = await res.text();
      let detailMsg = text || `HTTP ${res.status}`;
      try {
        const body = JSON.parse(text);
        if (typeof body.detail === "string") detailMsg = body.detail;
        else if (Array.isArray(body.detail))
          detailMsg = body.detail.map((x) => x.msg || JSON.stringify(x)).join("; ");
      } catch (_) {
        /* leave detailMsg */
      }
      if (!res.ok) throw new Error(detailMsg);
      setStatus("success");
      setName("");
      setEmail("");
      setMessage("");
      setCompany("");
    } catch (x) {
      setStatus("idle");
      setErr(x.message || "Something went wrong.");
    }
  }

  const sending = status === "sending";
  const canSubmit = enabled === true && !sending;

  return (
    <div className="card contact-card">
      <h2>Contact me</h2>
      <p className="small" style={{ marginTop: "-8px", marginBottom: "14px" }}>
        Send a note about this project or the ratings. Your email is only used to reply.
      </p>
      {enabled === false ? (
        <p className="small" style={{ marginBottom: 0 }}>
          Sending is not configured on this server yet (SMTP environment variables). Check{" "}
          <code>/api/health</code> for <code>contact_email</code> — it should read{" "}
          <code>configured</code> once env vars are set.
        </p>
      ) : (
        <form className="contact-form" onSubmit={handleSubmit}>
          <div className="field hp-field" aria-hidden="true">
            <label htmlFor="contact-company">Company</label>
            <input
              id="contact-company"
              name="company"
              type="text"
              tabIndex={-1}
              autoComplete="off"
              value={company}
              onChange={(e) => setCompany(e.target.value)}
            />
          </div>
          <div className="field">
            <label htmlFor="contact-name">Name</label>
            <input
              id="contact-name"
              name="name"
              type="text"
              required
              maxLength={200}
              autoComplete="name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              disabled={sending}
            />
          </div>
          <div className="field">
            <label htmlFor="contact-email">Email</label>
            <input
              id="contact-email"
              name="email"
              type="email"
              required
              maxLength={320}
              autoComplete="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              disabled={sending}
            />
          </div>
          <div className="field">
            <label htmlFor="contact-message">Message</label>
            <textarea
              id="contact-message"
              name="message"
              required
              maxLength={8000}
              rows={6}
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              disabled={sending}
            />
          </div>
          {err ? (
            <p className="small" style={{ color: "#f87171", marginBottom: "10px" }}>
              {err}
            </p>
          ) : null}
          {status === "success" ? (
            <p className="contact-success" style={{ marginBottom: "10px" }}>
              Thanks — your message was sent.
            </p>
          ) : null}
          <button type="submit" className="contact-send" disabled={!canSubmit}>
            {enabled === null ? "Checking…" : sending ? "Sending…" : "Send message"}
          </button>
        </form>
      )}
    </div>
  );
}

/** Renders narrative strings that use **markers** as bold (template-controlled; not raw HTML). */
function InfoPage({ navigate }) {
  return (
    <>
      <header className="page-hero">
        <h1>Method</h1>
        <p className="small">
          Short overview of how club <strong>ratings</strong> are produced and what you are seeing on the map and
          club pages.
        </p>
      </header>

      <div className="card">
        <h2>Rating system</h2>
        <p className="small" style={{ marginBottom: "12px" }}>
          Each club has a <strong>rating</strong> (strength estimate) and uncertainty that update after matches.
          The pipeline uses the <strong>Glicko-2</strong> algorithm — an extension of Elo meant for paired contests
          with sparse play — adapted here to football results grouped into <strong>rating weeks</strong> (not always
          one update per match day).
        </p>
        <p className="small" style={{ marginBottom: 0 }}>
          Higher <strong>rating</strong> ⇒ stronger expected performance vs typical opponents; margins and surprise
          results move ratings more than predictable wins.
        </p>
      </div>

      <div className="card">
        <h2>Global evidence layer (GCAM)</h2>
        <p className="small" style={{ marginBottom: 0 }}>
          After Glicko-2, an extra <strong>GCAM</strong> step summarises how broadly each club&apos;s results connect
          across leagues and competitions (<strong>connectivity</strong>), adds <strong>structural uncertainty</strong>{" "}
          when evidence is mostly locally clustered, and computes a <strong>trust-adjusted strength</strong> curve.
          Raw Glicko numbers stay in the dataset as the direct strength estimate; charts default to the adjusted curve
          when present so rankings reflect both skill and <strong>global comparability of evidence</strong>.
        </p>
      </div>

      <div className="card">
        <h2>Using the site</h2>
        <ul className="small info-list">
          <li>
            <strong>Map & top 25</strong> — hover countries, click for national snapshot and top-five rating
            tracks; open any club for full fixtures and weekly extremes.
          </li>
          <li>
            <strong>Ratings</strong> in tables and charts are comparable across clubs that share the same European
            run; they are descriptive, not betting advice.
          </li>
          <li>
            <strong>
              <a
                href="#/calibration"
                onClick={(e) => {
                  e.preventDefault();
                  navigate("/calibration");
                }}
              >
                Calibration
              </a>
            </strong>{" "}
            — empirical fit of predictions vs results (run{" "}
            <code>scripts/analyse_europe_calibration.py</code> after Glicko; optional{" "}
            <code>--last-weeks N</code> for the last N distinct rating weeks only).
          </li>
        </ul>
        <p style={{ marginBottom: 0, marginTop: "12px" }}>
          <a
            className="link-btn link-btn--primary"
            href="#/"
            onClick={(e) => {
              e.preventDefault();
              navigate("/");
            }}
          >
            ← Back to map & top 25
          </a>
        </p>
      </div>

      <div className="card">
        <h2>Country & club narratives</h2>
        <p className="small" style={{ marginBottom: "12px" }}>
          The prose blocks on country and club pages are <strong>generated automatically</strong> from the weekly
          rating CSVs (not hand-written). Highlights use the same visibility rules as the map and lists (recent
          activity per calendar year).
        </p>
        <ul className="small info-list" style={{ marginBottom: "12px" }}>
          <li>
            <strong>Templates</strong> — copy is rendered with{" "}
            <a href="https://jinja.palletsprojects.com/" target="_blank" rel="noreferrer">
              Jinja2
            </a>
            ; dates in narrative wording use{" "}
            <a href="https://pendulum.eustace.io/" target="_blank" rel="noreferrer">
              Pendulum
            </a>
            .
          </li>
          <li>
            <strong>Warm-up weeks</strong> — some ladder-style summaries (continental counts, domestic/European rank
            shares on club pages) ignore the first N chronological rating weeks so early mass ties near the default
            rating do not dominate rankings. The setting is{" "}
            <code>FOOTBALL_NARRATIVE_LADDER_DROP_FIRST_N_WEEKS</code> (default 52). Latest-week ranks on club pages
            still use the full series.
          </li>
          <li>
            <strong>Change-point segments</strong> — optional splits on average rating through time use{" "}
            <code>ruptures</code> (PELT, <code>l2</code> model) when that library is installed; otherwise a greedy
            split that minimizes within-segment squared error. Both are coarse summaries only.
          </li>
          <li>
            <strong>Country club highlights</strong> — examples: largest step from the previous rating week to the
            latest, highest peak rating, highest mean rating across weeks, and most weeks spent as the country&apos;s
            top-rated club that week. Tie-breaking uses rating then club name order where needed.
          </li>
          <li>
            <strong>Bold phrases</strong> in narratives are delimiter-based (<code>**like this**</code>), not raw
            HTML.
          </li>
        </ul>
        <p className="small" style={{ marginBottom: 0 }}>
          Each narrative API response also includes a <code>facts</code> object (counts, segment lengths, backend
          labels) for debugging or future UI — not shown on the page today.
        </p>
      </div>

      <ContactForm />
    </>
  );
}

function NarrativeParagraph({ text }) {
  const parts = String(text).split(/\*\*/);
  return (
    <p className="small" style={{ marginBottom: "12px", lineHeight: 1.65 }}>
      {parts.map((chunk, i) =>
        i % 2 === 1 ? (
          <strong key={i}>{chunk}</strong>
        ) : (
          <span key={i}>{chunk}</span>
        )
      )}
    </p>
  );
}

function CalibrationPage({ navigate, data, loading, error }) {
  const bins = data?.bins && Array.isArray(data.bins) ? data.bins : [];
  const gm = data?.global_metrics || {};
  const counts = data?.counts || {};

  const xSliderMax = useMemo(() => {
    if (!bins.length) return 1500;
    const mids = bins.map((b) => Math.abs(Number(b.rating_diff_mid) || 0));
    const m = Math.max(...mids, 0);
    return Math.min(2500, Math.max(500, Math.ceil((m + 100) / 25) * 25));
  }, [bins]);

  const [xAxisHalfSpan, setXAxisHalfSpan] = useState(500);
  const [overlayHomeWinOnMain, setOverlayHomeWinOnMain] = useState(true);

  useEffect(() => {
    setXAxisHalfSpan((prev) => Math.min(Math.max(prev, 50), xSliderMax));
  }, [xSliderMax]);

  const calibrationPlots = useMemo(() => {
    if (!bins.length) return { mainData: [], mainLayout: {}, rateData: [], rateLayout: {} };

    const x = bins.map((b) => b.rating_diff_mid);
    const customdata = bins.map((b) => [
      b.rating_diff_low,
      b.rating_diff_high,
      b.n,
      b.low_n ? "yes" : "no",
      b.mean_rating_diff,
    ]);

    const mutedAxis = {
      gridcolor: THEME.plotGrid,
      zerolinecolor: THEME.plotGrid,
      tickfont: { color: THEME.muted, size: 11 },
      color: THEME.muted,
      linecolor: THEME.plotGrid,
    };

    const baseMargin = { l: 54, r: 54, t: 28, b: 48 };

    const mainData = [
      {
        x,
        y: bins.map((b) => b.mean_actual_score),
        customdata,
        name: "Mean realised score",
        mode: "lines+markers",
        type: "scatter",
        line: { color: THEME.accent, width: 2 },
        marker: {
          size: bins.map((b) => (b.low_n ? 6 : 9)),
          color: bins.map((b) => (b.low_n ? "rgba(214,168,79,0.45)" : THEME.accent)),
          line: { width: 0 },
        },
        hovertemplate:
          "Rating diff (mid): %{x:.0f}<br>" +
          "Realised mean score: %{y:.3f}<br>" +
          "Bin [%{customdata[0]:.0f}, %{customdata[1]:.0f}), n=%{customdata[2]}, sparse bin: %{customdata[3]}<br>" +
          "Mean diff in bin: %{customdata[4]:.1f}<extra></extra>",
      },
      {
        x,
        y: bins.map((b) => b.mean_pred_pA),
        customdata,
        name: "Mean Glicko pred.",
        mode: "lines+markers",
        type: "scatter",
        line: { color: THEME.primaryBright, width: 2 },
        marker: {
          size: bins.map((b) => (b.low_n ? 6 : 9)),
          color: bins.map((b) => (b.low_n ? "rgba(96,165,250,0.45)" : THEME.primaryBright)),
          line: { width: 0 },
        },
        hovertemplate:
          "Rating diff (mid): %{x:.0f}<br>" +
          "Mean pred.: %{y:.3f}<br>" +
          "Bin [%{customdata[0]:.0f}, %{customdata[1]:.0f}), n=%{customdata[2]}<extra></extra>",
      },
      {
        x,
        y: bins.map((b) => b.mean_elo_expected_home),
        customdata,
        name: "Elo-400 baseline",
        mode: "lines+markers",
        type: "scatter",
        line: { color: THEME.muted, width: 1.5, dash: "dot" },
        marker: {
          size: bins.map((b) => (b.low_n ? 5 : 7)),
          color: THEME.muted,
          line: { width: 0 },
        },
        hovertemplate:
          "Rating diff (mid): %{x:.0f}<br>" +
          "Elo expectation: %{y:.3f}<br>" +
          "Bin [%{customdata[0]:.0f}, %{customdata[1]:.0f}), n=%{customdata[2]}<extra></extra>",
      },
    ];

    if (overlayHomeWinOnMain) {
      mainData.push({
        x,
        y: bins.map((b) => b.empirical_p_home_win),
        customdata,
        name: "Empirical P(home win)",
        mode: "lines+markers",
        type: "scatter",
        line: { color: "#C084FC", width: 2, dash: "longdash" },
        marker: {
          size: bins.map((b) => (b.low_n ? 5 : 8)),
          color: bins.map((b) => (b.low_n ? "rgba(192,132,252,0.45)" : "#C084FC")),
          symbol: "diamond",
          line: { width: 0 },
        },
        hovertemplate:
          "Rating diff (mid): %{x:.0f}<br>" +
          "Empirical P(home win): %{y:.3f}<br>" +
          "Bin [%{customdata[0]:.0f}, %{customdata[1]:.0f}), n=%{customdata[2]}<extra></extra>",
      });
    }

    const mainLayout = {
      font: { color: THEME.text },
      paper_bgcolor: THEME.plotPaper,
      plot_bgcolor: THEME.plotPaper,
      margin: baseMargin,
      showlegend: true,
      legend: {
        orientation: "h",
        yanchor: "bottom",
        y: 1.02,
        x: 0,
        font: { color: THEME.muted, size: 11 },
        bgcolor: "rgba(17,24,39,0.75)",
        bordercolor: "#334155",
        borderwidth: 1,
      },
      xaxis: {
        ...mutedAxis,
        title: { text: "Pre-match rating diff (home − away)", font: { color: THEME.muted, size: 12 } },
        range: [-xAxisHalfSpan, xAxisHalfSpan],
      },
      yaxis: {
        ...mutedAxis,
        title: {
          text: overlayHomeWinOnMain ? "Score / expectation / P(home)" : "Score / expectation",
          font: { color: THEME.muted, size: 12 },
        },
        range: [-0.05, 1.05],
      },
    };

    const rateData = [
      {
        x,
        y: bins.map((b) => b.empirical_p_home_win),
        customdata,
        name: "Empirical P(home win)",
        mode: "lines+markers",
        type: "scatter",
        line: { color: "#A78BFA", width: 2 },
        marker: { size: bins.map((b) => (b.low_n ? 6 : 9)), color: "#A78BFA" },
        hovertemplate:
          "Rating diff (mid): %{x:.0f}<br>" +
          "P(home win): %{y:.3f}<br>" +
          "n=%{customdata[2]}<extra></extra>",
      },
      {
        x,
        y: bins.map((b) => b.empirical_p_draw),
        customdata,
        name: "Empirical P(draw)",
        mode: "lines+markers",
        type: "scatter",
        line: { color: "#2DD4BF", width: 2 },
        marker: { size: bins.map((b) => (b.low_n ? 6 : 9)), color: "#2DD4BF" },
        hovertemplate:
          "Rating diff (mid): %{x:.0f}<br>" +
          "P(draw): %{y:.3f}<br>" +
          "n=%{customdata[2]}<extra></extra>",
      },
      {
        x,
        y: bins.map((b) => b.empirical_p_away_win),
        customdata,
        name: "Empirical P(away win)",
        mode: "lines+markers",
        type: "scatter",
        line: { color: "#FB923C", width: 2 },
        marker: { size: bins.map((b) => (b.low_n ? 6 : 9)), color: "#FB923C" },
        hovertemplate:
          "Rating diff (mid): %{x:.0f}<br>" +
          "P(away win): %{y:.3f}<br>" +
          "n=%{customdata[2]}<extra></extra>",
      },
    ];

    const rateLayout = {
      font: { color: THEME.text },
      paper_bgcolor: THEME.plotPaper,
      plot_bgcolor: THEME.plotPaper,
      margin: baseMargin,
      showlegend: true,
      legend: {
        orientation: "h",
        yanchor: "bottom",
        y: 1.02,
        x: 0,
        font: { color: THEME.muted, size: 11 },
        bgcolor: "rgba(17,24,39,0.75)",
        bordercolor: "#334155",
        borderwidth: 1,
      },
      xaxis: {
        ...mutedAxis,
        title: { text: "Pre-match rating diff (home − away)", font: { color: THEME.muted, size: 12 } },
        range: [-xAxisHalfSpan, xAxisHalfSpan],
      },
      yaxis: {
        ...mutedAxis,
        title: { text: "Empirical share", font: { color: THEME.muted, size: 12 } },
        range: [-0.02, 1.02],
      },
    };

    return { mainData, mainLayout, rateData, rateLayout };
  }, [bins, xAxisHalfSpan, overlayHomeWinOnMain]);

  return (
    <>
      <nav className="page-nav" aria-label="Breadcrumb">
        <a
          className="link-btn"
          href="#/"
          onClick={(e) => {
            e.preventDefault();
            navigate("/");
          }}
        >
          ← Map & rankings
        </a>
      </nav>

      <header className="page-hero">
        <p className="sub-head">Model diagnostics</p>
        <h1>Prediction calibration</h1>
        <p className="small">
          Matches are grouped by <strong>home pre-rating minus away pre-rating</strong> (same snapshots as the upset
          heuristic). Compare mean <strong>realised score</strong> (win&nbsp;=&nbsp;1, draw&nbsp;=&nbsp;0.5, loss
          &nbsp;=&nbsp;0) to the engine&apos;s mean <strong>Glicko expectation</strong> and a simple Elo-style curve.
        </p>
      </header>

      {loading ? (
        <div className="card card-muted loading-pulse">
          <p>Loading calibration…</p>
        </div>
      ) : error ? (
        <div className="card error">
          <p style={{ marginBottom: "12px" }}>{error}</p>
          <p className="small" style={{ marginBottom: "14px" }}>
            Generate JSON first from the repo root:{" "}
            <code style={{ wordBreak: "break-all" }}>python scripts/analyse_europe_calibration.py</code>
            then reload the API (<code>POST /api/reload</code> if the server was already running).
          </p>
          <a
            className="link-btn"
            href="#/"
            onClick={(e) => {
              e.preventDefault();
              navigate("/");
            }}
          >
            Back home
          </a>
        </div>
      ) : !bins.length ? (
        <div className="card card-muted">
          <p>No calibration bins in the payload.</p>
        </div>
      ) : (
        <>
          <div className="card">
            <h2>Summary</h2>
            {(() => {
              const filt = data?.filters || {};
              const nUsed =
                counts.merged_rows_used_after_week_filter != null
                  ? counts.merged_rows_used_after_week_filter
                  : counts.merged_rows_used_after_dropna != null
                    ? counts.merged_rows_used_after_dropna
                    : null;
              const nDropna =
                counts.merged_rows_after_dropna != null ? counts.merged_rows_after_dropna : nUsed;
              return (
                <>
                  <p className="small" style={{ marginTop: "-8px", marginBottom: "10px" }}>
                    Bin width <strong>{data?.bin_width ?? "—"}</strong> rating points
                    {nUsed != null ? (
                      <>
                        {" "}
                        · <strong>{Number(nUsed).toLocaleString()}</strong> matches in calibration
                        {filt.applied ? (
                          <>
                            {" "}
                            (last <strong>{filt.distinct_weeks_used}</strong> distinct rating weeks{" "}
                            <strong>{filt.week_id_min}</strong>–<strong>{filt.week_id_max}</strong>)
                          </>
                        ) : null}
                      </>
                    ) : null}
                    {data?.generated_at ? (
                      <>
                        {" "}
                        · Generated <strong>{String(data.generated_at).slice(0, 19).replace("T", " ")}</strong> UTC
                      </>
                    ) : null}
                  </p>
                  {filt.applied && filt.truncated_to_all_available ? (
                    <p className="small" style={{ marginTop: 0, marginBottom: "10px", color: THEME.muted }}>
                      Requested last <strong>{filt.last_weeks_requested}</strong> rating weeks; file has only{" "}
                      <strong>{filt.distinct_weeks_available}</strong> distinct weeks — used entire span.
                    </p>
                  ) : null}
                  {filt.applied && nDropna != null && nUsed != null && Number(nDropna) > Number(nUsed) ? (
                    <p className="small" style={{ marginTop: 0, marginBottom: "10px", color: THEME.muted }}>
                      <strong>{Number(nDropna).toLocaleString()}</strong> rows after merge/dropna before the week
                      window.
                    </p>
                  ) : null}
                </>
              );
            })()}
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Metric</th>
                    <th>Glicko pred.</th>
                    <th>Elo-400 baseline</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>MAE (vs realised score)</td>
                    <td className="rating-strong">
                      {gm.mae_expected_score_glicko_pred != null
                        ? Number(gm.mae_expected_score_glicko_pred).toFixed(4)
                        : "—"}
                    </td>
                    <td>
                      {gm.mae_expected_score_elo400_baseline != null
                        ? Number(gm.mae_expected_score_elo400_baseline).toFixed(4)
                        : "—"}
                    </td>
                  </tr>
                  <tr>
                    <td>RMSE</td>
                    <td>
                      {gm.rmse_expected_score_glicko_pred != null
                        ? Number(gm.rmse_expected_score_glicko_pred).toFixed(4)
                        : "—"}
                    </td>
                    <td>
                      {gm.rmse_expected_score_elo400_baseline != null
                        ? Number(gm.rmse_expected_score_elo400_baseline).toFixed(4)
                        : "—"}
                    </td>
                  </tr>
                  <tr>
                    <td>Mean realised score / mean pred.</td>
                    <td colSpan={2} className="small">
                      {gm.mean_actual_score != null ? Number(gm.mean_actual_score).toFixed(4) : "—"} actual vs{" "}
                      {gm.mean_pred_pA != null ? Number(gm.mean_pred_pA).toFixed(4) : "—"} pred.
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <div className="card">
            <h2>Chart window</h2>
            <p className="small" style={{ marginTop: "-8px", marginBottom: "14px" }}>
              Horizontal axis shows pre-match rating difference (home − away). Drag to zoom out for blowouts or zoom in
              on tight matches.
            </p>
            <label htmlFor="cal-x-span-slider" style={{ display: "block", marginBottom: "10px" }}>
              <span className="small" style={{ fontWeight: 600, color: "var(--text-muted)" }}>
                Half-span ±{xAxisHalfSpan} pts
              </span>
              <span className="small" style={{ marginLeft: "8px", color: THEME.muted }}>
                (full width {-xAxisHalfSpan} … +{xAxisHalfSpan})
              </span>
            </label>
            <input
              id="cal-x-span-slider"
              type="range"
              min={50}
              max={xSliderMax}
              step={25}
              value={xAxisHalfSpan}
              onChange={(e) => setXAxisHalfSpan(Number(e.target.value))}
              aria-valuemin={50}
              aria-valuemax={xSliderMax}
              aria-valuenow={xAxisHalfSpan}
              aria-label="Calibration charts horizontal axis half-span in rating points"
              style={{
                width: "100%",
                maxWidth: "520px",
                accentColor: "var(--primary-bright)",
                cursor: "pointer",
              }}
            />
            <p className="kbd-hint" style={{ marginTop: "10px", marginBottom: 0 }}>
              Range up to ±{xSliderMax} from loaded bins (step 25).
            </p>
          </div>

          <div className="card">
            <h2>Mean score by rating gap</h2>
            <p className="small" style={{ marginTop: "-8px", marginBottom: "12px" }}>
              <strong>X:</strong> bin centre <code>rating_diff_mid</code> (home − away pre-rating).{" "}
              <strong>Y:</strong> <code>mean_actual_score</code> vs <code>mean_pred_pA</code> (plus Elo baseline).
              All share the 0–1 vertical scale so empirical home-win rate is comparable to mean score when overlaid.
            </p>
            <label
              className="small"
              style={{
                display: "flex",
                alignItems: "center",
                gap: "10px",
                cursor: "pointer",
                marginBottom: "14px",
                userSelect: "none",
              }}
            >
              <input
                type="checkbox"
                checked={overlayHomeWinOnMain}
                onChange={(e) => setOverlayHomeWinOnMain(e.target.checked)}
              />
              Overlay <strong>empirical_p_home_win</strong> (diamonds, dashed purple)
            </label>
            <p className="small" style={{ marginTop: "-6px", marginBottom: "12px", color: THEME.muted }}>
              Fainter markers = sparse bins. Good calibration: realised mean tracks Glicko pred.; P(home win) rises
              with rating advantage but need not equal mean score (draws count in mean score only).
            </p>
            <Plot data={calibrationPlots.mainData} layout={calibrationPlots.mainLayout} />
          </div>

          <div className="card">
            <h2>Empirical W/D/A shares</h2>
            <p className="small" style={{ marginTop: "-8px" }}>
              Outcome frequencies within each rating-difference bin (home perspective).
            </p>
            <Plot data={calibrationPlots.rateData} layout={calibrationPlots.rateLayout} />
          </div>

          <div className="card">
            <h2>Notes</h2>
            <ul className="small" style={{ marginTop: 0, paddingLeft: "1.2rem", lineHeight: 1.55 }}>
              {(data?.notes && Array.isArray(data.notes) ? data.notes : []).map((line, i) => (
                <li key={`cal-note-${i}`} style={{ marginBottom: "8px" }}>
                  {line}
                </li>
              ))}
            </ul>
            <p style={{ marginBottom: 0 }}>
              <a
                className="link-btn link-btn--primary"
                href="#/info"
                onClick={(e) => {
                  e.preventDefault();
                  navigate("/info");
                }}
              >
                Method & narrative tooling → Info
              </a>
            </p>
          </div>
        </>
      )}
    </>
  );
}

function App() {
  const [countries, setCountries] = useState([]);
  const [teams, setTeams] = useState([]);
  const [countrySummaries, setCountrySummaries] = useState([]);
  const [selectedCountry, setSelectedCountry] = useState("");
  const [selectedTeamId, setSelectedTeamId] = useState("");
  const [hoveredCountry, setHoveredCountry] = useState("");

  const [teamSeries, setTeamSeries] = useState([]);
  const [countryTopSeries, setCountryTopSeries] = useState(null);
  const [countryNarrative, setCountryNarrative] = useState(null);
  const [biggestMatches, setBiggestMatches] = useState({ upsets: [], swings: [] });
  const [topSnapshot, setTopSnapshot] = useState([]);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [clubDetail, setClubDetail] = useState(null);
  const [clubNarrative, setClubNarrative] = useState(null);
  const [clubLoading, setClubLoading] = useState(false);
  const [clubError, setClubError] = useState("");

  const [calibrationData, setCalibrationData] = useState(null);
  const [calibrationLoading, setCalibrationLoading] = useState(false);
  const [calibrationError, setCalibrationError] = useState("");

  const { route, navigate, hash } = useHashRoute();

  const mapHostRef = React.useRef(null);
  const [mapDims, setMapDims] = useState(() => {
    if (typeof window === "undefined") return { w: 1200, h: 560 };
    const w = Math.min(Math.floor(window.innerWidth * 0.94), 1680);
    const aspect = 2.15;
    const h = Math.round(Math.min(Math.max(w / aspect, 340), 760));
    return { w, h };
  });

  useEffect(() => {
    const el = mapHostRef.current;
    if (!el || typeof ResizeObserver === "undefined") return;

    const measure = () => {
      const rect = el.getBoundingClientRect();
      const w = Math.floor(rect.width);
      const h = Math.floor(rect.height);
      if (w < 80 || h < 80) return;
      setMapDims((prev) => (prev.w === w && prev.h === h ? prev : { w, h }));
    };

    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const selectedTeam = useMemo(
    () => teams.find((team) => String(team.pid) === String(selectedTeamId)),
    [teams, selectedTeamId]
  );

  useLayoutEffect(() => {
    if (route.page !== "calibration") return;
    setCalibrationLoading(true);
    setCalibrationError("");
  }, [route.page, hash]);

  useEffect(() => {
    if (route.page !== "calibration") return;
    let cancelled = false;
    getJson("/api/calibration")
      .then((d) => {
        if (!cancelled) {
          setCalibrationData(d);
          setCalibrationLoading(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setCalibrationData(null);
          setCalibrationError(err.message || "Failed to load calibration.");
          setCalibrationLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [route.page, hash]);

  useEffect(() => {
    async function init() {
      try {
        setLoading(true);
        const [countriesData, teamsData, snapshot, summariesData] = await Promise.all([
          getJson("/api/countries"),
          getJson("/api/teams"),
          getJson("/api/snapshot?top_n=25"),
          getJson("/api/country-summaries", { allow404: true }),
        ]);
        setCountries(countriesData);
        setTeams(teamsData);
        setTopSnapshot(snapshot);
        setCountrySummaries(Array.isArray(summariesData) ? summariesData : []);
        const initial = typeof window !== "undefined" ? parseHashRoute() : { page: "home" };
        if (countriesData.length > 0) {
          if (initial.page === "club" && initial.teamId) {
            const tm = teamsData.find((t) => String(t.pid) === String(initial.teamId));
            if (tm) {
              setSelectedTeamId(String(tm.pid));
              setSelectedCountry(String(tm.country_name).toLowerCase());
            }
          } else if (initial.page === "country" && initial.country) {
            const ok = countriesData.some((c) => String(c).toLowerCase() === initial.country);
            if (ok) setSelectedCountry(initial.country);
            else setSelectedCountry(countriesData[0]);
          } else {
            setSelectedCountry(countriesData[0]);
          }
        }
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    }
    init();
  }, []);

  useEffect(() => {
    const parsed = parseHashRouteFromString(hash);
    if (parsed.page !== "country" || !parsed.country) {
      setCountryTopSeries(null);
      setCountryNarrative(null);
      return;
    }
    const slug = parsed.country;
    let cancelled = false;

    async function loadCountryTopFive() {
      try {
        const [topPayload, countryTeams, narrativePayload] = await Promise.all([
          getJson(`/api/country/${slug}/top-timeseries`, { allow404: true }),
          getJson(`/api/teams?country=${slug}`),
          getJson(`/api/country/${slug}/narrative`, { allow404: true }),
        ]);
        if (cancelled) return;
        const payload =
          topPayload && Array.isArray(topPayload.teams) ? topPayload : { teams: [] };
        setCountryTopSeries(payload);
        setCountryNarrative(
          narrativePayload && Array.isArray(narrativePayload.paragraphs)
            ? narrativePayload
            : null
        );
        const topIds = payload.teams.map((t) => String(t.pid));
        const firstPick =
          topIds.find((id) => countryTeams.some((t) => String(t.pid) === id)) ||
          (countryTeams[0] ? String(countryTeams[0].pid) : "");
        setSelectedTeamId(firstPick);
      } catch (err) {
        if (!cancelled) setError(err.message);
      }
    }

    loadCountryTopFive();
    return () => {
      cancelled = true;
    };
  }, [hash]);

  useEffect(() => {
    const parsed = parseHashRouteFromString(hash);
    if (parsed.page === "club" && parsed.teamId) {
      setSelectedTeamId(parsed.teamId);
    }
  }, [hash]);

  useEffect(() => {
    const parsed = parseHashRouteFromString(hash);
    if (parsed.page !== "club" || !parsed.teamId) {
      setClubDetail(null);
      setClubNarrative(null);
      setClubLoading(false);
      setClubError("");
      return;
    }
    let cancelled = false;
    setClubLoading(true);
    setClubDetail(null);
    setClubNarrative(null);
    setClubError("");
    (async () => {
      try {
        const detailPromise = fetchClubDetailWithFallbacks(parsed.teamId, 240000);
        const narrativePromise = getJson(`/api/team/${parsed.teamId}/narrative`, {
          allow404: true,
        });
        const [detailOut, narrativeOut] = await Promise.allSettled([
          detailPromise,
          narrativePromise,
        ]);

        if (cancelled) return;

        if (detailOut.status === "rejected") {
          setClubLoading(false);
          setClubError(detailOut.reason?.message || "Failed to load club");
          return;
        }

        setClubDetail(detailOut.value);
        setClubLoading(false);

        if (
          narrativeOut.status === "fulfilled" &&
          narrativeOut.value &&
          Array.isArray(narrativeOut.value.paragraphs) &&
          narrativeOut.value.paragraphs.length > 0
        ) {
          setClubNarrative(narrativeOut.value);
        }
      } catch (err) {
        if (!cancelled) {
          setClubLoading(false);
          setClubError(err.message || "Failed to load club");
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [hash]);

  useEffect(() => {
    if (!selectedTeamId) return;
    const parsed = parseHashRouteFromString(hash);
    if (parsed.page === "country") return;

    let cancelled = false;

    async function loadTeamDetail() {
      try {
        const [series, matches] = await Promise.all([
          getJson(`/api/team/${selectedTeamId}/timeseries`, { allow404: true }),
          getJson(`/api/team/${selectedTeamId}/biggest-matches?limit=12`, { allow404: true }),
        ]);
        if (cancelled) return;
        setTeamSeries(Array.isArray(series) ? series : []);
        setBiggestMatches(matches || { upsets: [], swings: [] });
      } catch (err) {
        if (!cancelled) setError(err.message);
      }
    }

    loadTeamDetail();
    return () => {
      cancelled = true;
    };
  }, [selectedTeamId, hash]);

  useEffect(() => {
    if (route.page !== "country" || !route.country || !countries.length) return;
    const ok = countries.some((c) => String(c).toLowerCase() === route.country);
    if (ok) setSelectedCountry(route.country);
  }, [route.page, route.country, countries]);

  const filteredTeams = useMemo(() => {
    const slug =
      route.page === "country" && route.country ? route.country : selectedCountry;
    if (!slug) return teams;
    return teams.filter((team) => team.country_name.toLowerCase() === slug.toLowerCase());
  }, [teams, selectedCountry, route.page, route.country]);

  const summaryByCountry = useMemo(() => {
    const map = new Map();
    countrySummaries.forEach((item) => map.set(item.country_name.toLowerCase(), item));
    return map;
  }, [countrySummaries]);

  const selectedCountrySummary = selectedCountry
    ? summaryByCountry.get(selectedCountry.toLowerCase())
    : null;

  const hoveredCountrySummary = hoveredCountry
    ? summaryByCountry.get(hoveredCountry.toLowerCase())
    : null;

  const mapPoints = useMemo(() => {
    const withSummaries = countrySummaries
      .map((summary) => {
        const key = String(summary.country_name || "").toLowerCase();
        const coords = COUNTRY_MAP_COORDS[key];
        if (!coords) return null;
        return { ...summary, ...coords };
      })
      .filter(Boolean);

    if (withSummaries.length > 0) {
      return withSummaries;
    }

    // Fallback so map remains interactive even if summaries endpoint is unavailable.
    return countries
      .map((country) => {
        const key = String(country || "").toLowerCase();
        const coords = COUNTRY_MAP_COORDS[key];
        if (!coords) return null;
        return {
          country_name: country,
          average_rating: 1500,
          active_teams: 0,
          top_team_name: "N/A",
          top_team_rating: 1500,
          ...coords,
        };
      })
      .filter(Boolean);
  }, [countrySummaries, countries]);

  const heatVals = mapPoints.map(mapHeatValue);
  const minHeat = heatVals.length > 0 ? Math.min(...heatVals) : 0;
  const maxHeat = heatVals.length > 0 ? Math.max(...heatVals) : 1;

  const markerSizes = mapPoints.map((p, i) => {
    const v = heatVals[i];
    if (maxHeat === minHeat) return 16;
    return 12 + ((v - minHeat) / (maxHeat - minHeat)) * 16;
  });

  const choroplethPoints = mapPoints.filter(
    (p) => CHOROPLETH_LOCATION_BY_COUNTRY[String(p.country_name || "").toLowerCase()]
  );

  const markerOnlyPoints = mapPoints.filter(
    (p) => !CHOROPLETH_LOCATION_BY_COUNTRY[String(p.country_name || "").toLowerCase()]
  );

  const mapData = [
    {
      type: "choropleth",
      locationmode: "country names",
      locations: choroplethPoints.map(
        (p) => CHOROPLETH_LOCATION_BY_COUNTRY[String(p.country_name || "").toLowerCase()]
      ),
      z: choroplethPoints.map(mapHeatValue),
      customdata: choroplethPoints.map((p) => [
        p.country_name,
        p.average_rating,
        p.active_teams,
        p.top_team_name,
        p.top_team_rating,
      ]),
      hovertemplate:
        "<b>%{location}</b><br>" +
        "Best team: %{customdata[3]} (%{customdata[4]:.1f})<br>" +
        "Avg rating: %{customdata[1]:.1f}<br>" +
        "Active teams: %{customdata[2]}<extra></extra>",
      colorscale: MAP_HEAT_COLORSCALE,
      zmin: minHeat,
      zmax: maxHeat,
      marker: { line: { color: "#334155", width: 0.8 } },
      colorbar: {
        orientation: "h",
        x: 0.5,
        xanchor: "center",
        y: -0.02,
        yanchor: "top",
        len: 0.62,
        thickness: 14,
        title: {
          text: "Best team rating",
          font: { color: THEME.muted, size: 12 },
          side: "bottom",
        },
        tickfont: { color: THEME.muted, size: 11 },
        bgcolor: "rgba(17,24,39,0.92)",
        bordercolor: "#334155",
        borderwidth: 1,
      },
    },
    {
      type: "scattergeo",
      mode: "markers+text",
      showlegend: false,
      lat: markerOnlyPoints.map((p) => p.lat),
      lon: markerOnlyPoints.map((p) => p.lon),
      text: markerOnlyPoints.map((p) => p.label),
      textposition: "top center",
      customdata: markerOnlyPoints.map((p) => [
        p.country_name,
        p.average_rating,
        p.active_teams,
        p.top_team_name,
        p.top_team_rating,
      ]),
      hovertemplate:
        "<b>%{text}</b><br>" +
        "Best team: %{customdata[3]} (%{customdata[4]:.1f})<br>" +
        "Avg rating: %{customdata[1]:.1f}<br>" +
        "Active teams: %{customdata[2]}<extra></extra>",
      marker: {
        size: markerOnlyPoints.map((p) => {
          const idx = mapPoints.findIndex((m) => m.country_name === p.country_name);
          return idx >= 0 ? markerSizes[idx] : 14;
        }),
        color: markerOnlyPoints.map(mapHeatValue),
        colorscale: MAP_HEAT_COLORSCALE,
        cmin: minHeat,
        cmax: maxHeat,
        line: { color: "#475569", width: 1.2 },
        opacity: 0.95,
        showscale: false,
      },
    },
  ];

  const europeMapLayout = useMemo(
    () => ({
      autosize: false,
      width: mapDims.w,
      height: mapDims.h,
      font: { color: THEME.text },
      paper_bgcolor: THEME.plotPaper,
      geo: {
        scope: "europe",
        projection: { type: "mercator" },
        showland: true,
        landcolor: THEME.geoLand,
        showcountries: true,
        countrycolor: THEME.geoBorder,
        showocean: true,
        oceancolor: THEME.geoOcean,
        /* Tighter N–S than default scope → geographic aspect reads wider inside the plot (less “square”). */
        lataxis: { range: [37, 62] },
        lonaxis: { range: [-24, 46] },
        domain: { x: [0.02, 0.98], y: [0.02, 0.88] },
      },
      margin: { l: 4, r: 12, t: 12, b: 72 },
    }),
    [mapDims.w, mapDims.h]
  );

  const countryTopFivePlotLayout = {
    font: { color: THEME.text },
    paper_bgcolor: THEME.plotPaper,
    plot_bgcolor: THEME.plotGrid,
    legend: {
      orientation: "h",
      yanchor: "bottom",
      y: 1.02,
      xanchor: "left",
      x: 0,
      font: { color: THEME.muted, size: 11 },
      bgcolor: "rgba(17,24,39,0.75)",
      bordercolor: "#334155",
      borderwidth: 1,
    },
    xaxis: {
      title: { text: "Week start", font: { color: THEME.muted, size: 12 } },
      gridcolor: "#334155",
      zerolinecolor: "#334155",
      tickfont: { color: THEME.muted, size: 11 },
      color: THEME.muted,
      linecolor: "#334155",
    },
    yaxis: {
      title: { text: "Rating", font: { color: THEME.muted, size: 12 } },
      gridcolor: "#334155",
      zerolinecolor: "#334155",
      tickfont: { color: THEME.muted, size: 11 },
      color: THEME.muted,
      linecolor: "#334155",
    },
    margin: { l: 50, r: 20, t: 24, b: 40 },
  };

  const teamStrengthKey =
    teamSeries.length && teamSeries[0].adjusted_rating != null ? "adjusted_rating" : "rating";

  const teamPlotLayout = {
    font: { color: THEME.text },
    paper_bgcolor: THEME.plotPaper,
    plot_bgcolor: THEME.plotGrid,
    legend: {
      font: { color: THEME.muted, size: 12 },
      bgcolor: "rgba(17,24,39,0.75)",
      bordercolor: "#334155",
      borderwidth: 1,
    },
    xaxis: {
      title: { text: "Date", font: { color: THEME.muted, size: 12 } },
      gridcolor: "#334155",
      zerolinecolor: "#334155",
      tickfont: { color: THEME.muted, size: 11 },
      color: THEME.muted,
      linecolor: "#334155",
    },
    yaxis: {
      title: {
        text: teamStrengthKey === "adjusted_rating" ? "Adjusted strength (GCAM)" : "Rating",
        font: { color: THEME.muted, size: 12 },
      },
      gridcolor: "#334155",
      zerolinecolor: "#334155",
      tickfont: { color: THEME.muted, size: 11 },
      color: THEME.muted,
      linecolor: "#334155",
    },
    margin: { l: 50, r: 20, t: 20, b: 40 },
  };

  const teamTrendData = [
    {
      x: teamSeries.map((d) => d.week_date),
      y: teamSeries.map((d) => Number(d[teamStrengthKey] ?? d.rating)),
      mode: "lines",
      type: "scatter",
      name: selectedTeam ? selectedTeam.team_name : "Team",
      line: { color: THEME.primaryBright, width: 1.35, dash: "dot" },
    },
  ];

  const countryTopFivePlotData = useMemo(() => {
    const teams = countryTopSeries?.teams;
    if (!teams || teams.length === 0) return [];
    return teams.map((t, i) => {
      const c = COUNTRY_TOP5_LINE_COLORS[i % COUNTRY_TOP5_LINE_COLORS.length];
      return {
        x: t.series.map((p) => p.week_date),
        y: t.series.map((p) => p.rating),
        mode: "lines",
        type: "scatter",
        name: t.team_name,
        line: { color: c, width: 1.35, dash: "dot" },
      };
    });
  }, [countryTopSeries]);

  const countryPageSummary =
    route.page === "country" && route.country ? summaryByCountry.get(route.country) : null;
  const countrySlugKnown =
    route.page !== "country" ||
    !route.country ||
    countries.length === 0 ||
    countries.some((c) => String(c).toLowerCase() === route.country);

  const clubCountrySlug =
    clubDetail && clubDetail.country_name
      ? String(clubDetail.country_name).toLowerCase()
      : selectedTeam
        ? String(selectedTeam.country_name).toLowerCase()
        : "";

  return (
    <>
      <header className="site-header">
        <div className="site-header-inner">
          <a
            href="#/"
            className="site-brand"
            onClick={(e) => {
              e.preventDefault();
              navigate("/");
            }}
          >
            <span className="site-brand-mark" aria-hidden />
            Football rankings
          </a>
          <nav className="site-nav" aria-label="Primary">
            <a
              className="link-btn link-btn--header"
              href="#/"
              onClick={(e) => {
                e.preventDefault();
                navigate("/");
              }}
            >
              Map & top 25
            </a>
            <a
              className="link-btn link-btn--header"
              href="#/calibration"
              onClick={(e) => {
                e.preventDefault();
                navigate("/calibration");
              }}
            >
              Calibration
            </a>
            <a
              className="link-btn link-btn--header"
              href="#/info"
              onClick={(e) => {
                e.preventDefault();
                navigate("/info");
              }}
            >
              Info
            </a>
          </nav>
        </div>
      </header>
      <main id="main-content" className="container">
      {error && route.page !== "club" && route.page !== "info" && route.page !== "calibration" && (
        <div className="card error">{error}</div>
      )}

      {route.page === "info" ? (
        <InfoPage navigate={navigate} />
      ) : route.page === "calibration" ? (
        <CalibrationPage
          navigate={navigate}
          data={calibrationData}
          loading={calibrationLoading}
          error={calibrationError}
        />
      ) : route.page === "club" ? (
        <>
          <nav className="page-nav" aria-label="Breadcrumb">
            <a
              className="link-btn"
              href="#/"
              onClick={(e) => {
                e.preventDefault();
                navigate("/");
              }}
            >
              ← Map & rankings
            </a>
            {clubCountrySlug ? (
              <a
                className="link-btn"
                href={`#/country/${encodeURIComponent(clubCountrySlug)}`}
                onClick={(e) => {
                  e.preventDefault();
                  navigate(`/country/${encodeURIComponent(clubCountrySlug)}`);
                }}
              >
                ← {formatCountryDisplay(clubCountrySlug)}
              </a>
            ) : null}
          </nav>

          {clubLoading || (loading && !clubDetail && !clubError) ? (
            <div className="card card-muted loading-pulse">
              <p>Loading club data…</p>
              <p className="small" style={{ marginBottom: 0 }}>
                First load reads match history from disk and can take{" "}
                <strong>30 seconds to a few minutes</strong>. If your terminal shows{" "}
                <code>FOOTBALL_RANKINGS_SKIP_PRELOAD=1</code>, that preload was skipped and this step will be slower.
                <br />
                Sanity check (opens new tab):{" "}
                {route.page === "club" && route.teamId ? (
                  <a
                    href={`/api/teams/${route.teamId}/identity`}
                    target="_blank"
                    rel="noreferrer"
                  >
                    /api/teams/{route.teamId}/identity
                  </a>
                ) : null}{" "}
                ·{" "}
                <a href="/api/health" target="_blank" rel="noreferrer">
                  /api/health
                </a>
              </p>
            </div>
          ) : clubError ? (
            <div className="card error" style={{ whiteSpace: "pre-wrap" }}>
              {clubError}{" "}
              <a
                className="link-btn"
                href="#/"
                onClick={(e) => {
                  e.preventDefault();
                  navigate("/");
                }}
              >
                Back home
              </a>
            </div>
          ) : clubDetail ? (
            <>
              <h1>{clubDetail.team_name}</h1>
              <p className="small">
                {formatCountryDisplay(clubDetail.country_name)} · Full match history and the biggest single-match
                rating swings (updates are applied per <strong>rating week</strong>, not always per match date).
              </p>

              {clubNarrative && clubNarrative.paragraphs?.length ? (
                <div className="card">
                  <h2>Club narrative</h2>
                  {clubNarrative.paragraphs.map((para, i) => (
                    <NarrativeParagraph key={`club-nar-${i}`} text={para} />
                  ))}
                </div>
              ) : null}

              <div className="card">
                <h2>Rating over time</h2>
                <Plot data={teamTrendData} layout={teamPlotLayout} />
              </div>

              <div className="club-extremes-grid">
                <div className="card">
                  <h2>Largest rating gains</h2>
                  <p className="small" style={{ marginTop: "-8px" }}>
                    Fixtures with the biggest positive rating change for this club (post-match rating shown).
                  </p>
                  <div className="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Date</th>
                          <th>Opposition</th>
                          <th>Comp</th>
                          <th>Rating</th>
                          <th>Δ rating</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(clubDetail.rating_gains || []).map((row, i) => (
                          <tr key={`gain-${row.match_date}-${row.opponent_name}-${i}`}>
                            <td>{row.match_date || "—"}</td>
                            <td>{row.opponent_name}</td>
                            <td>
                              <CompetitionBadge code={row.competition} />
                            </td>
                            <td>{Number(row.rating).toFixed(1)}</td>
                            <td className="numeric-pos">{formatSignedRating(row.rating_change)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div className="card">
                  <h2>Largest rating losses</h2>
                  <p className="small" style={{ marginTop: "-8px" }}>
                    Fixtures with the most negative rating change (post-match rating shown).
                  </p>
                  <div className="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Date</th>
                          <th>Opposition</th>
                          <th>Comp</th>
                          <th>Rating</th>
                          <th>Δ rating</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(clubDetail.rating_losses || []).map((row, i) => (
                          <tr key={`loss-${row.match_date}-${row.opponent_name}-${i}`}>
                            <td>{row.match_date || "—"}</td>
                            <td>{row.opponent_name}</td>
                            <td>
                              <CompetitionBadge code={row.competition} />
                            </td>
                            <td>{Number(row.rating).toFixed(1)}</td>
                            <td className="numeric-neg">{formatSignedRating(row.rating_change)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>

              <div className="card">
                <h2>All results ({clubDetail.matches.length})</h2>
                <p className="small" style={{ marginTop: "-8px" }}>
                  Every match in the dataset involving this club (newest first). Δ rating is this
                  club&apos;s change from that fixture.
                </p>
                <div className="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Date</th>
                        <th>Venue</th>
                        <th>Scoreline</th>
                        <th>Opponent</th>
                        <th>Comp</th>
                        <th>Δ rating</th>
                        <th>Pre → post</th>
                      </tr>
                    </thead>
                    <tbody>
                      {clubDetail.matches.map((row, index) => (
                        <tr key={`${row.week}-${row.match_date}-${row.opponent_id}-${index}`}>
                          <td>{row.match_date || "—"}</td>
                          <td>{row.venue}</td>
                          <td>
                            {row.team_goals}-{row.opponent_goals}
                          </td>
                          <td>{row.opponent_name}</td>
                          <td>
                            <CompetitionBadge code={row.competition} />
                          </td>
                          <td
                            className={
                              Number(row.rating_change) >= 0 ? "numeric-pos" : "numeric-neg"
                            }
                          >
                            {formatSignedRating(row.rating_change)}
                          </td>
                          <td className="small">
                            {Number(row.pre_rating).toFixed(1)} → {Number(row.post_rating).toFixed(1)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          ) : null}
        </>
      ) : route.page === "country" ? (
        <>
          <nav className="page-nav" aria-label="Breadcrumb">
            <a
              className="link-btn"
              href="#/"
              onClick={(e) => {
                e.preventDefault();
                navigate("/");
              }}
            >
              ← Map & rankings
            </a>
          </nav>

          {loading || countries.length === 0 ? (
            <div className="card card-muted loading-pulse">Loading country…</div>
          ) : !countrySlugKnown ? (
            <div className="card error">
              Unknown country “{formatCountryDisplay(route.country)}”.{" "}
              <a
                className="link-btn"
                href="#/"
                onClick={(e) => {
                  e.preventDefault();
                  navigate("/");
                }}
              >
                Back home
              </a>
            </div>
          ) : (
            <>
              <h1>
                {formatCountryDisplay(
                  countryPageSummary ? countryPageSummary.country_name : route.country
                )}
              </h1>
              <p className="small">
                Nation-level snapshot and how today&apos;s strongest clubs evolved week by week. Use the team
                picker to open a club&apos;s full match history.
              </p>

              {countryPageSummary && (
                <div className="card">
                  <h2>Summary</h2>
                  <p className="small">
                    Average rating {countryPageSummary.average_rating.toFixed(1)} across{" "}
                    {countryPageSummary.active_teams} active teams. Best club:{" "}
                    {countryPageSummary.top_team_name} ({countryPageSummary.top_team_rating.toFixed(1)}).
                  </p>
                </div>
              )}

              {countryNarrative && countryNarrative.paragraphs?.length ? (
                <div className="card">
                  <h2>Country narrative</h2>
                  {countryNarrative.paragraphs.map((para, i) => (
                    <NarrativeParagraph key={`nar-${i}`} text={para} />
                  ))}
                </div>
              ) : null}

              <div className="card controls">
                <div>
                  <label>Team</label>
                  <select value={selectedTeamId} onChange={(e) => setSelectedTeamId(e.target.value)}>
                    {filteredTeams.length === 0 ? (
                      <option value="">No clubs match visibility rules for this country</option>
                    ) : (
                      filteredTeams.map((team) => (
                        <option key={team.pid} value={team.pid}>
                          {team.team_name}
                        </option>
                      ))
                    )}
                  </select>
                </div>
                {selectedTeamId ? (
                  <div style={{ alignSelf: "end" }}>
                    <a
                      className="link-btn link-btn--primary"
                      href={`#/club/${selectedTeamId}`}
                      onClick={(e) => {
                        e.preventDefault();
                        navigate(`/club/${selectedTeamId}`);
                      }}
                    >
                      Open club page → full results & weekly extremes
                    </a>
                  </div>
                ) : null}
              </div>

              <div className="card">
                <h2>Current top 5 — history over time</h2>
                <p className="small" style={{ marginTop: "-8px" }}>
                  The five highest-rated clubs in this country in the latest rating week; each line follows that
                  club across all rating weeks in the dataset.
                </p>
                {countryTopFivePlotData.length === 0 ? (
                  <p className="small" style={{ marginBottom: 0 }}>
                    No weekly series loaded yet — wait for data or pick another country.
                  </p>
                ) : (
                  <Plot data={countryTopFivePlotData} layout={countryTopFivePlotLayout} />
                )}
              </div>
            </>
          )}
        </>
      ) : (
        <>
          <header className="page-hero">
            <p className="sub-head">Ratings · European clubs</p>
            <h1>Ratings dashboard</h1>
            <p className="small">
              Hover countries for a snapshot; <strong>click</strong> to open detail. Rankings below stay in sync with the latest rating week.
            </p>
          </header>

          <section className="map-full-width" aria-label="European ratings map">
            <div className="card map-card">
              <div className="map-card-intro">
                <h2>European Ratings Map</h2>
                <p className="small" style={{ marginTop: 0, paddingBottom: "12px" }}>
                  Shading shows each nation&apos;s <strong>strongest</strong> club — deeper slate blues are weaker;
                  brighter royal blues are stronger. Hover for a snapshot; <strong>click</strong> a country to open its
                  page.
                </p>
              </div>
              <div ref={mapHostRef} className="map-plot-host">
                <Plot
                  data={mapData}
                  layout={europeMapLayout}
                  onClick={(event) => {
                    const point = event.points?.[0];
                    if (!point || !point.customdata) return;
                    const slug = String(point.customdata[0]).toLowerCase();
                    setSelectedCountry(slug);
                    navigate(`/country/${encodeURIComponent(slug)}`);
                  }}
                  onHover={(event) => {
                    const point = event.points?.[0];
                    if (!point || !point.customdata) return;
                    setHoveredCountry(point.customdata[0]);
                  }}
                />
              </div>
              <p className="small map-card-status">
                {hoveredCountrySummary
                  ? (() => {
                      const br = Number(hoveredCountrySummary.top_team_rating);
                      const best = Number.isFinite(br) ? br.toFixed(1) : "—";
                      return `${formatCountryDisplay(hoveredCountrySummary.country_name)}: best ${hoveredCountrySummary.top_team_name} (${best}), avg ${hoveredCountrySummary.average_rating.toFixed(1)}, ${hoveredCountrySummary.active_teams} teams — click map to open page.`;
                    })()
                  : "Hover any marker to preview that nation; click to open its summary."}
              </p>
            </div>
          </section>

          {loading && (
            <div className="card card-muted loading-pulse" aria-busy="true">
              Loading data…
            </div>
          )}

          <div className="card">
            <h2>Current top 25</h2>
            <p className="small" style={{ marginTop: "-8px", marginBottom: "14px" }}>
              Latest rating week. Only clubs with more than five matches in each of 2024, 2025, and 2026 appear here
              and on the map (see About). Rows are clickable — open a club&apos;s full history.
            </p>
            <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>#</th>
                  <th>Team</th>
                  <th>Country</th>
                  <th>{topSnapshot.some((r) => r.adjusted_rating != null) ? "Strength (adj.)" : "Rating"}</th>
                  <th>{topSnapshot.some((r) => r.total_rd != null) ? "RD (total)" : "RD"}</th>
                </tr>
              </thead>
              <tbody>
                {topSnapshot.map((row, idx) => (
                  <tr
                    key={row.pid}
                    className="click-row"
                    tabIndex={0}
                    role="button"
                    aria-label={`Open ${row.team_name}, ranked ${idx + 1}`}
                    onClick={() => navigate(`/club/${row.pid}`)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        navigate(`/club/${row.pid}`);
                      }
                    }}
                  >
                    <td>
                      <span className="rank-cell">{idx + 1}</span>
                    </td>
                    <td>{row.team_name}</td>
                    <td>{formatCountryDisplay(row.country_name)}</td>
                    <td className="rating-strong">
                      {(row.adjusted_rating != null ? Number(row.adjusted_rating) : Number(row.rating)).toFixed(1)}
                    </td>
                    <td>{(row.total_rd != null ? Number(row.total_rd) : Number(row.rd)).toFixed(1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            </div>
            <p className="kbd-hint">Tip: press Enter on a focused row to open the club page.</p>
          </div>
        </>
      )}
      </main>
    </>
  );
}

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<App />);
