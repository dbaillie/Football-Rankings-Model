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

/**
 * Weekly diffused-strength Y values for the club chart (see backend `diffused_weekly_column`).
 */
function weeklyDiffusedY(teamSeries) {
  if (!teamSeries.length) return null;
  const preferSimple = teamSeries.some((d) => {
    const v = d.simple_adjusted_rating;
    return v != null && v !== "" && Number.isFinite(Number(v));
  });
  const key = preferSimple ? "simple_adjusted_rating" : "adjusted_rating";
  const ys = teamSeries.map((d) => {
    const v = d[key];
    if (v == null || v === "") return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  });
  return ys.some((y) => y != null) ? ys : null;
}

/** Choropleth / markers: weak → strong (all blue — no green in the heat ramp) */
const MAP_HEAT_COLORSCALE = [
  [0, "#1e293b"],
  [0.2, "#1e3a5f"],
  [0.45, "#2563eb"],
  [0.68, "#3b82f6"],
  [0.88, "#60a5fa"],
  [1, "#93c5fd"],
];

/** Glicko μ for snapshot table sorting and display. */
function snapshotRawValue(row) {
  const v = Number(row.rating);
  return Number.isFinite(v) ? v : null;
}

function formatSnapshotStrengthCell(value) {
  if (value == null || Number.isNaN(value)) return "—";
  return value.toFixed(1);
}

/**
 * Comparator for sorting snapshot rows by Glicko rating.
 * Returns numeric compare (a − b); nulls sort after finite values for both ascending and descending sorts.
 */
function compareSnapshotNumeric(aNum, bNum) {
  const aMiss = aNum === null || Number.isNaN(aNum);
  const bMiss = bNum === null || Number.isNaN(bNum);
  if (aMiss && bMiss) return 0;
  if (aMiss) return 1;
  if (bMiss) return -1;
  return aNum - bNum;
}

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

function friendlyHttpStatusMessage(status) {
  if (status === 404) return "That content wasn't found.";
  if (status >= 500) return "The service is temporarily unavailable. Please try again.";
  return "Something went wrong. Please try again.";
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
      throw new Error(friendlyHttpStatusMessage(response.status));
    }
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch {
      throw new Error("Something went wrong loading data. Please try again.");
    }
  } catch (err) {
    if (err.name === "AbortError") {
      throw new Error("This took too long. Please try again.");
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
  for (const path of urls) {
    try {
      return await getJson(path, { timeoutMs });
    } catch {
      /* try next path */
    }
  }
  throw new Error("We couldn't load this club's page. Please try again.");
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
  if (segments[0] === "diffused") {
    return { page: "diffused" };
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
      await res.text();
      if (!res.ok) throw new Error(friendlyHttpStatusMessage(res.status));
      setStatus("success");
      setName("");
      setEmail("");
      setMessage("");
      setCompany("");
    } catch {
      setStatus("idle");
      setErr("Your message couldn't be sent. Please try again later or use email.");
    }
  }

  const sending = status === "sending";
  const canSubmit = enabled === true && !sending;

  return (
    <div className="card contact-card">
      <h2>Contact Me</h2>
      <p className="small" style={{ marginTop: "-8px", marginBottom: "14px" }}>
        Send a note about this project or the ratings. Your email is only used to reply.
      </p>
      {enabled === false ? (
        <p className="small" style={{ marginBottom: 0 }}>
          The contact form isn&apos;t available on this deployment yet. Please use the email address on the Info page.
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

/** Conceptual overview of “diffused” / comparability strength (not plotted on the main dashboard). */
function DiffusedPage({ navigate }) {
  return (
    <>
      <header className="page-hero">
        <h1>Diffused Strength</h1>
        <p className="small">
          Why an optional layer exists next to raw Glicko, and why this site keeps browsing on{" "}
          <strong>rating</strong> (μ).
        </p>
      </header>

      <div className="card">
        <h2>Raw Glicko First</h2>
        <p className="small" style={{ marginBottom: "12px" }}>
          Glicko-2 produces a mean strength <strong>μ</strong> and uncertainty for each club from results. That update is
          the authoritative sporting signal: it is tuned for prediction within the rating system and respects sparse play.
        </p>
        <p className="small" style={{ marginBottom: 0 }}>
          The dashboard map, country charts, club trajectories, top table, and generated narratives therefore read{" "}
          <strong>rating</strong> so what you see matches the core model output.
        </p>
      </div>

      <div className="card">
        <h2>What “Diffused” Means Here</h2>
        <p className="small" style={{ marginBottom: "12px" }}>
          Clubs in different leagues rarely face the same opponent pool. Raw μ ranks everyone inside one European run, but
          interpreting <em>how hard</em> a path looked — domestic-only vs heavy European minutes — is a separate
          question from the week-to-week Glicko step.
        </p>
        <p className="small" style={{ marginBottom: 0 }}>
          The <strong>simple adjusted</strong> / comparability layer treats schedule exposure a bit like diffusion across
          contexts: strength estimates can be nudged toward anchors informed by who you played and where (with shrink when
          the signal is thin). It is a descriptive lens for cross-context storytelling, not a replacement for the Glicko
          update itself.
        </p>
      </div>

      <div className="card">
        <h2>Where to Read More</h2>
        <p className="small" style={{ marginBottom: 0 }}>
          Background and formulas are on{" "}
          <a
            href="#/info"
            onClick={(e) => {
              e.preventDefault();
              navigate("/info");
            }}
          >
            Info
          </a>
          . To return to the live ratings UI:{" "}
          <a
            href="#/"
            onClick={(e) => {
              e.preventDefault();
              navigate("/");
            }}
          >
            Map &amp; Top 25 Clubs
          </a>
          .
        </p>
      </div>
    </>
  );
}

/** Public-facing explainers for ratings and the dashboard (legacy bundle). */
function InfoPage({ navigate }) {
  return (
    <>
      <header className="page-hero">
        <h1>How the Ratings Work</h1>
        <p className="small">
          What the numbers mean, how they&apos;re produced, and how to read this site — without diving into
          implementation detail.
        </p>
      </header>

      <div className="card">
        <h2>Rating System</h2>
        <p className="small" style={{ marginBottom: "12px" }}>
          Each club has a <strong>rating</strong> (strength estimate) and uncertainty that update after matches.
          The model uses <strong>Glicko-2</strong>, an extension of Elo suited to intermittent play: results are
          rolled into <strong>rating weeks</strong>, so updates may bundle several fixtures rather than firing after
          every single match day.
        </p>
        <p className="small" style={{ marginBottom: 0 }}>
          A higher <strong>rating</strong> means stronger expected results against typical opponents; big surprises and
          tight margins move ratings more than routine wins.
        </p>
      </div>

      <div className="card">
        <h2>Glicko-2 — Core Ideas</h2>
        <p className="small" style={{ marginBottom: "12px" }}>
          Each club has a mean strength <strong>μ</strong>, an uncertainty band around it (<strong>φ</strong>,
          sometimes called rating deviation), and a volatility term <strong>σ</strong>. Each week, wins, draws, and
          losses feed an update step. The formulas below are the standard compact sketch; full theory is in Mark
          Glickman&apos;s Glicko-2 paper.
        </p>
        <p className="small" style={{ marginBottom: "8px" }}>
          <strong>Opponent uncertainty</strong> enters match expectations through:
        </p>
        <div className="info-equation" aria-label="RD damping formula">
          <span className="info-equation-label">Opponent scaling factor</span>
          {`g(φ_j) = √( 1 + 3φ_j² / π² )`}
        </div>
        <p className="small" style={{ marginBottom: "8px", marginTop: "14px" }}>
          <strong>Expected score</strong> for side <em>i</em> versus <em>j</em> (same logistic shape as Elo, adjusted
          for <em>j</em>&apos;s uncertainty):
        </p>
        <div className="info-equation" aria-label="Expected score formula">
          <span className="info-equation-label">Match expectation</span>
          {`E_ij = 1 / ( 1 + 10^( -( g(φ_j)(μ_i - μ_j) ) / 400 ) )`}
        </div>
        <p className="small" style={{ marginBottom: "8px", marginTop: "14px" }}>
          Actual outcomes are compared to <strong>E</strong>; surprise results and volatility shape how far{" "}
          <strong>μ</strong>, <strong>φ</strong>, and <strong>σ</strong> move before the next week.
        </p>
        <p className="small" style={{ marginBottom: 0 }}>
          On this site, <strong>maps, charts, rankings, and automated write-ups</strong> use that raw Glicko strength
          (the rating, μ). For an optional lens that folds schedule context into one comparable curve — what we call{" "}
          <strong>diffused</strong> strength — see the{" "}
          <a
            href="#/diffused"
            onClick={(e) => {
              e.preventDefault();
              navigate("/diffused");
            }}
          >
            Diffused
          </a>{" "}
          page.
        </p>
      </div>

      <div className="card">
        <h2>Schedule Comparability (Optional Layer)</h2>
        <p className="small" style={{ marginBottom: 0 }}>
          Behind the scenes the project can also derive <strong>simple adjusted strength</strong> after Glicko —
          blending cross-league schedule exposure and optional strength-of-schedule anchors.{" "}
          <strong>That is not what you see</strong> on the main dashboard; it is mainly for downloads and research. For
          the intuition, read{" "}
          <a
            href="#/diffused"
            onClick={(e) => {
              e.preventDefault();
              navigate("/diffused");
            }}
          >
            Diffused
          </a>
          .
        </p>
      </div>

      <div className="card">
        <h2>Using This Site</h2>
        <ul className="small info-list">
          <li>
            <strong>Dashboard</strong> — explore Europe on the map, compare countries, open clubs for fixtures and
            rating history, and browse the current top table.
          </li>
          <li>
            <strong>Diffused</strong> — separate explainer on schedule-diffusion / comparability (not the default
            strength curve here).
          </li>
          <li>
            <strong>Ratings</strong> describe historical strength in one continuous European run; they are not betting
            tips or guarantees about future results.
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
            — checks how well model forecasts line up with actual scores and outcomes across pre-match rating gaps.
          </li>
        </ul>
      </div>

      <div className="card">
        <h2>Automated Country &amp; Club Notes</h2>
        <p className="small" style={{ marginBottom: "12px" }}>
          The short prose blocks on country and club pages are <strong>generated from the same rating history</strong>{" "}
          as the charts — they are not hand-edited match reports. Only clubs with enough matches in recent seasons
          appear on the map and in those summaries.
        </p>
        <ul className="small info-list" style={{ marginBottom: "12px" }}>
          <li>
            <strong>Ladder-style statistics</strong> (for example domestic vs European rank bands over time) trim the
            earliest stretch of weekly snapshots so early-season clustering near the starting rating does not swamp
            long-run trends. Headline “latest week” figures still use the full history.
          </li>
          <li>
            <strong>Era segments</strong> — where shown, time splits are an automatic summary of stronger vs weaker
            stretches of form; treat them as coarse guides, not precise breakpoints.
          </li>
          <li>
            <strong>Country highlights</strong> might include big week-to-week moves, peak ratings, averages across
            time, or how often a club led its nation on the ladder — with sensible tie-breaking when clubs are close.
          </li>
        </ul>
        <p className="small" style={{ marginBottom: 0 }}>
          Highlighted phrases in the text use simple markup so emphasis stays readable and consistent.
        </p>
      </div>

      <div className="card">
        <h2>About</h2>
        <p className="small" style={{ marginBottom: 0 }}>
          Built by <strong>Douglas Baillie</strong>. Contact:{" "}
          <a href="mailto:douglasbaillie@live.co.uk">douglasbaillie@live.co.uk</a>
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
      b.low_n ? "(few matches)" : "",
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
          "Rating gap (centre): %{x:.0f}<br>" +
          "Mean score (0–1): %{y:.3f}<br>" +
          "Band [%{customdata[0]:.0f}, %{customdata[1]:.0f}) · %{customdata[2]} matches %{customdata[3]}<br>" +
          "Mean rating gap in band: %{customdata[4]:.1f}<extra></extra>",
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
          "Rating gap (centre): %{x:.0f}<br>" +
          "Model expectation: %{y:.3f}<br>" +
          "Band [%{customdata[0]:.0f}, %{customdata[1]:.0f}) · %{customdata[2]} matches %{customdata[3]}<extra></extra>",
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
          "Rating gap (centre): %{x:.0f}<br>" +
          "Reference curve: %{y:.3f}<br>" +
          "Band [%{customdata[0]:.0f}, %{customdata[1]:.0f}) · %{customdata[2]} matches %{customdata[3]}<extra></extra>",
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
          "Rating gap (centre): %{x:.0f}<br>" +
          "Observed home-win rate: %{y:.3f}<br>" +
          "Band [%{customdata[0]:.0f}, %{customdata[1]:.0f}) · %{customdata[2]} matches %{customdata[3]}<extra></extra>",
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
          "Rating gap (centre): %{x:.0f}<br>" +
          "Home wins: %{y:.3f}<br>" +
          "%{customdata[2]} matches %{customdata[3]}<extra></extra>",
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
          "Rating gap (centre): %{x:.0f}<br>" +
          "Draws: %{y:.3f}<br>" +
          "%{customdata[2]} matches %{customdata[3]}<extra></extra>",
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
          "Rating gap (centre): %{x:.0f}<br>" +
          "Away wins: %{y:.3f}<br>" +
          "%{customdata[2]} matches %{customdata[3]}<extra></extra>",
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
          ← Explorer
        </a>
      </nav>

      <header className="page-hero">
        <p className="sub-head">Forecast Quality</p>
        <h1>Prediction Calibration</h1>
        <p className="small">
          Fixtures are grouped by how much stronger the home side was on paper before kick-off (home rating minus away
          rating). For each band you can compare typical <strong>results</strong> (win&nbsp;=&nbsp;1, draw&nbsp;=&nbsp;0.5,
          loss&nbsp;=&nbsp;0) with the model&apos;s average expectation and a simple reference curve — a sanity check
          that forecasts behave sensibly across mismatches.
        </p>
      </header>

      {loading ? (
        <div className="card card-muted loading-pulse">
          <p>Loading calibration charts…</p>
        </div>
      ) : error ? (
        <div className="card error">
          <p style={{ marginBottom: "14px" }}>{error}</p>
          <a
            className="link-btn"
            href="#/"
            onClick={(e) => {
              e.preventDefault();
              navigate("/");
            }}
          >
            Back Home
          </a>
        </div>
      ) : !bins.length ? (
        <div className="card card-muted">
          <p>Calibration charts aren&apos;t available yet.</p>
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
              return (
                <>
                  <p className="small" style={{ marginTop: "-8px", marginBottom: "10px" }}>
                    Each band is <strong>{data?.bin_width ?? "—"}</strong> rating points wide
                    {nUsed != null ? (
                      <>
                        {" "}
                        · based on <strong>{Number(nUsed).toLocaleString()}</strong> matches
                        {filt.applied ? (
                          <>
                            {" "}
                            (rating weeks <strong>{filt.week_id_min}</strong>–<strong>{filt.week_id_max}</strong>,{" "}
                            <strong>{filt.distinct_weeks_used}</strong> weeks)
                          </>
                        ) : null}
                      </>
                    ) : null}
                    {data?.generated_at ? (
                      <>
                        {" "}
                        · snapshot <strong>{String(data.generated_at).slice(0, 19).replace("T", " ")}</strong> UTC
                      </>
                    ) : null}
                  </p>
                  {filt.applied && filt.truncated_to_all_available ? (
                    <p className="small" style={{ marginTop: 0, marginBottom: "10px", color: THEME.muted }}>
                      Fewer weekly snapshots exist than requested; all available history in the dataset was used (
                      <strong>{filt.distinct_weeks_available}</strong> weeks).
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
                    <th>Glicko Pred.</th>
                    <th>Elo-400 Baseline</th>
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
                    <td>Mean outcome vs mean forecast</td>
                    <td colSpan={2} className="small">
                      {gm.mean_actual_score != null ? Number(gm.mean_actual_score).toFixed(4) : "—"} realised vs{" "}
                      {gm.mean_pred_pA != null ? Number(gm.mean_pred_pA).toFixed(4) : "—"} predicted.
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <div className="card">
            <h2>Chart Window</h2>
            <p className="small" style={{ marginTop: "-8px", marginBottom: "14px" }}>
              Horizontal axis is how much stronger the home side was rated before kick-off (home minus away). Use the
              slider to widen or tighten the window around evenly matched games versus heavy favourites.
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
              aria-label="Horizontal chart range around evenly matched games"
              style={{
                width: "100%",
                maxWidth: "520px",
                accentColor: "var(--primary-bright)",
                cursor: "pointer",
              }}
            />
            <p className="kbd-hint" style={{ marginTop: "10px", marginBottom: 0 }}>
              Maximum span ±{xSliderMax} rating points (steps of 25).
            </p>
          </div>

          <div className="card">
            <h2>Mean Score by Rating Gap</h2>
            <p className="small" style={{ marginTop: "-8px", marginBottom: "12px" }}>
              Horizontal axis: pre-match rating gap (home minus away). Vertical axis: average outcome score (0–1 scale)
              compared with the model expectation and a reference curve. Optional overlay adds observed home-win rate on
              the same scale for context.
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
              Overlay <strong>observed home-win rate</strong> (diamond markers, purple dashed line)
            </label>
            <p className="small" style={{ marginTop: "-6px", marginBottom: "12px", color: THEME.muted }}>
              Fainter markers indicate fewer games in that band. Well behaved forecasts sit close to realised outcomes;
              home-win rate should rise when the home side is favoured, but need not match the mean score line because
              draws sit in the middle of that scale.
            </p>
            <Plot data={calibrationPlots.mainData} layout={calibrationPlots.mainLayout} />
          </div>

          <div className="card">
            <h2>Empirical W/D/A Shares</h2>
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
                How the Ratings Work (Info)
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
  /** Client reorder of top snapshot by Glicko rating only (server default is newest-week desc). */
  const [snapshotRatingSortDir, setSnapshotRatingSortDir] = useState("desc");

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
      .catch(() => {
        if (!cancelled) {
          setCalibrationData(null);
          setCalibrationError("Calibration isn't available right now. Please try again later.");
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
      } catch {
        setError("We couldn't load the dashboard. Please refresh the page.");
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
      } catch {
        if (!cancelled) setError("We couldn't load this country's data. Please try again.");
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
          setClubError("We couldn't load this club's page. Please try again.");
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
      } catch {
        if (!cancelled) {
          setClubLoading(false);
          setClubError("We couldn't load this club's page. Please try again.");
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
      } catch {
        if (!cancelled) setError("We couldn't load this team's chart data. Please try again.");
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

  const sortedTopSnapshot = useMemo(() => {
    if (!topSnapshot.length) return [];
    const rows = topSnapshot.slice();
    const tieBreak = (a, b) =>
      String(a.team_name || "").localeCompare(String(b.team_name || ""), undefined, {
        sensitivity: "base",
      });
    rows.sort((a, b) => {
      const cmp = compareSnapshotNumeric(snapshotRawValue(a), snapshotRawValue(b));
      if (cmp !== 0) return snapshotRatingSortDir === "desc" ? -cmp : cmp;
      return tieBreak(a, b);
    });
    return rows;
  }, [topSnapshot, snapshotRatingSortDir]);

  const toggleSnapshotRatingSort = React.useCallback(() => {
    setSnapshotRatingSortDir((d) => (d === "desc" ? "asc" : "desc"));
  }, []);

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
          text: "Best team Glicko rating",
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
      title: { text: "Rating (μ & diffused)", font: { color: THEME.muted, size: 12 } },
      gridcolor: "#334155",
      zerolinecolor: "#334155",
      tickfont: { color: THEME.muted, size: 11 },
      color: THEME.muted,
      linecolor: "#334155",
    },
    margin: { l: 50, r: 20, t: 56, b: 40 },
  };

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
      title: { text: "Rating (μ & diffused)", font: { color: THEME.muted, size: 12 } },
      gridcolor: "#334155",
      zerolinecolor: "#334155",
      tickfont: { color: THEME.muted, size: 11 },
      color: THEME.muted,
      linecolor: "#334155",
    },
    margin: { l: 50, r: 20, t: 24, b: 40 },
  };

  const teamTrendData = useMemo(() => {
    const baseName = selectedTeam ? selectedTeam.team_name : "Team";
    const x = teamSeries.map((d) => d.week_date);
    const traces = [
      {
        x,
        y: teamSeries.map((d) => Number(d.rating)),
        mode: "lines",
        type: "scatter",
        name: `${baseName} (Glicko μ)`,
        line: { color: THEME.primaryBright, width: 1.35 },
      },
    ];
    const diffY = weeklyDiffusedY(teamSeries);
    if (diffY) {
      traces.push({
        x,
        y: diffY,
        mode: "lines",
        type: "scatter",
        name: `${baseName} (diffused)`,
        line: { color: THEME.accent, width: 1.35, dash: "dash" },
      });
    }
    return traces;
  }, [teamSeries, selectedTeam]);

  const countryTopFivePlotData = useMemo(() => {
    const teams = countryTopSeries?.teams;
    if (!teams || teams.length === 0) return [];
    return teams.flatMap((t, i) => {
      const c = COUNTRY_TOP5_LINE_COLORS[i % COUNTRY_TOP5_LINE_COLORS.length];
      const x = t.series.map((p) => p.week_date);
      const glicko = {
        x,
        y: t.series.map((p) => p.rating),
        mode: "lines",
        type: "scatter",
        name: `${t.team_name} (μ)`,
        line: { color: c, width: 1.35 },
      };
      const hasDiffused = t.series.some((p) => {
        const v = p.diffused_rating;
        return v != null && Number.isFinite(Number(v));
      });
      if (!hasDiffused) return [glicko];
      return [
        glicko,
        {
          x,
          y: t.series.map((p) => p.diffused_rating),
          mode: "lines",
          type: "scatter",
          name: `${t.team_name} (diffused)`,
          line: { color: c, width: 1.35, dash: "dash" },
        },
      ];
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
            <img
              className="site-brand-mark"
              src="/marble-mark.svg"
              alt=""
              width={32}
              height={32}
              decoding="async"
            />
            Football Ratings
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
              Map & Top 25 Clubs
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
              href="#/diffused"
              onClick={(e) => {
                e.preventDefault();
                navigate("/diffused");
              }}
            >
              Diffused
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
      {error &&
        route.page !== "club" &&
        route.page !== "info" &&
        route.page !== "diffused" &&
        route.page !== "calibration" && (
        <div className="card error">{error}</div>
      )}

      {route.page === "info" ? (
        <InfoPage navigate={navigate} />
      ) : route.page === "diffused" ? (
        <DiffusedPage navigate={navigate} />
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
              ← Explorer
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
              <p>Loading club profile…</p>
              <p className="small" style={{ marginBottom: 0 }}>
                This may take a little longer on first visit while match history is prepared.
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
                Back Home
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
                  <h2>Club Narrative</h2>
                  {clubNarrative.paragraphs.map((para, i) => (
                    <NarrativeParagraph key={`club-nar-${i}`} text={para} />
                  ))}
                </div>
              ) : null}

              <div className="card">
                <h2>Rating Over Time</h2>
                <p className="small" style={{ marginTop: "-8px" }}>
                  Solid: Glicko μ (weekly rating). Dashed: diffused strength when available in the dataset.
                </p>
                <Plot data={teamTrendData} layout={teamPlotLayout} />
              </div>

              <div className="club-extremes-grid">
                <div className="card">
                  <h2>Largest Rating Gains</h2>
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
                          <th>Δ Rating</th>
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
                  <h2>Largest Rating Losses</h2>
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
                          <th>Δ Rating</th>
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
                <h2>All Results ({clubDetail.matches.length})</h2>
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
                        <th>Δ Rating</th>
                        <th>Pre → Post</th>
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
              ← Explorer
            </a>
          </nav>

          {loading || countries.length === 0 ? (
            <div className="card card-muted loading-pulse">Loading country profile…</div>
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
                Back Home
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
                Nation-level snapshot and how today&apos;s highest-rated clubs evolved week by week. Use the team
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
                  <h2>Country Narrative</h2>
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
                      <option value="">No clubs listed for this country right now</option>
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
                      Open Club Page — Full Results & Weekly Extremes
                    </a>
                  </div>
                ) : null}
              </div>

              <div className="card">
                <h2>Current Top 5 — History Over Time</h2>
                <p className="small" style={{ marginTop: "-8px" }}>
                  The five highest-rated clubs in this country in the latest rating week; each line follows that club
                  across all rating weeks. Solid: Glicko μ; dashed: diffused strength (same color per club), when
                  available.
                </p>
                {countryTopFivePlotData.length === 0 ? (
                  <p className="small" style={{ marginBottom: 0 }}>
                    Chart data isn&apos;t available for this view yet. Try again shortly or choose another country.
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
            <p className="sub-head">Ratings · European Clubs</p>
            <h1>Ratings Dashboard</h1>
            <p className="small">
              Hover countries for a snapshot; <strong>click</strong> to open detail. The table below uses{" "}
              <strong>Glicko rating</strong> for the latest rating week (see{" "}
              <a
                href="#/diffused"
                onClick={(e) => {
                  e.preventDefault();
                  navigate("/diffused");
                }}
              >
                Diffused
              </a>{" "}
              for an optional comparability lens).
            </p>
          </header>

          <section className="map-full-width" aria-label="European ratings map">
            <div className="card map-card">
              <div className="map-card-intro">
                <h2>European Ratings Map</h2>
                <p className="small" style={{ marginTop: 0, paddingBottom: "12px" }}>
                  Shading shows each nation&apos;s <strong>highest-rated</strong> club — deeper slate blues are weaker;
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
              <p style={{ margin: 0 }}>Loading Football Ratings…</p>
            </div>
          )}

          <div className="card">
            <h2>Current Top 25 Clubs</h2>
            <p className="small" style={{ marginTop: "-8px", marginBottom: "14px" }}>
              Latest rating week by <strong>Glicko rating</strong>. Only clubs with more than five matches in each of 2024,
              2025, and 2026 appear here and on the map (see About). Rows are clickable — open a club&apos;s full history.
              Click the rating header to flip ascending / descending (default matches server order).
            </p>
            <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>#</th>
                  <th>Team</th>
                  <th>Country</th>
                  <th scope="col" aria-sort={snapshotRatingSortDir === "desc" ? "descending" : "ascending"}>
                    <button
                      type="button"
                      className="th-sort-btn"
                      title="Sort by Glicko rating"
                      onClick={(e) => {
                        e.stopPropagation();
                        toggleSnapshotRatingSort();
                      }}
                    >
                      Glicko rating
                      <span className="th-sort-indicator" aria-hidden>
                        {snapshotRatingSortDir === "desc" ? "▼" : "▲"}
                      </span>
                    </button>
                  </th>
                  <th>{topSnapshot.some((r) => r.total_rd != null) ? "RD (total)" : "RD"}</th>
                </tr>
              </thead>
              <tbody>
                {sortedTopSnapshot.map((row, idx) => (
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
                    <td className="rating-strong">{formatSnapshotStrengthCell(snapshotRawValue(row))}</td>
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
