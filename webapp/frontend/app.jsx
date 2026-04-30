const { useEffect, useMemo, useState } = React;

/** Matches light theme in index.html (Stadium daylight) */
const THEME = {
  text: "#121826",
  muted: "#5c6778",
  primary: "#1e5eff",
  accent: "#e85d04",
  success: "#0f766e",
  geoLand: "#eef2f6",
  geoBorder: "#c5ced9",
  geoOcean: "#d9e6ff",
  plotPaper: "#ffffff",
  plotGrid: "#eef1f5",
};

/** Choropleth / markers: low (weak) → high (strong); light blue → dark blue */
const MAP_HEAT_COLORSCALE = [
  [0, "#e8f4fc"],
  [0.25, "#bfdbfe"],
  [0.5, "#60a5fa"],
  [0.75, "#2563eb"],
  [1, "#172554"],
];

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
      className={className}
      style={{ width: "100%", minHeight: className === "map-plot-host" ? undefined : 420 }}
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
  /* UEFA / national-team style rows use dim country "International" — not on choropleth, marker-only */
  international: { lat: 46.2, lon: 8.3, label: "International (UEFA)" },
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

function formatMatch(row) {
  return `${row.home_team_name} ${row.home_goals}-${row.away_goals} ${row.away_team_name}`;
}

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

function App() {
  const [countries, setCountries] = useState([]);
  const [teams, setTeams] = useState([]);
  const [countrySummaries, setCountrySummaries] = useState([]);
  const [selectedCountry, setSelectedCountry] = useState("");
  const [selectedTeamId, setSelectedTeamId] = useState("");
  const [hoveredCountry, setHoveredCountry] = useState("");

  const [teamSeries, setTeamSeries] = useState([]);
  const [countrySeries, setCountrySeries] = useState([]);
  const [biggestMatches, setBiggestMatches] = useState({ upsets: [], swings: [] });
  const [topSnapshot, setTopSnapshot] = useState([]);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [clubDetail, setClubDetail] = useState(null);
  const [clubLoading, setClubLoading] = useState(false);
  const [clubError, setClubError] = useState("");

  const { route, navigate, hash } = useHashRoute();

  const mapHostRef = React.useRef(null);
  const [mapDims, setMapDims] = useState(() => {
    if (typeof window === "undefined") return { w: 1200, h: 680 };
    const w = Math.min(Math.floor(window.innerWidth * 0.94), 1680);
    const h = Math.round(Math.min(Math.max(w * 0.62, 460), 820));
    return { w, h };
  });

  useEffect(() => {
    const el = mapHostRef.current;
    if (!el || typeof ResizeObserver === "undefined") return;

    const measure = () => {
      const w = Math.floor(el.getBoundingClientRect().width);
      if (w < 80) return;
      const h = Math.round(Math.min(Math.max(w * 0.62, 460), 820));
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
    if (parsed.page !== "country" || !parsed.country) return;
    const slug = parsed.country;
    let cancelled = false;

    async function loadCountrySeries() {
      try {
        const [countryData, countryTeams] = await Promise.all([
          getJson(`/api/country/${slug}/timeseries`),
          getJson(`/api/teams?country=${slug}`),
        ]);
        if (cancelled) return;
        setCountrySeries(countryData);
        if (countryTeams.length > 0) {
          setSelectedTeamId(String(countryTeams[0].pid));
        } else {
          setSelectedTeamId("");
        }
      } catch (err) {
        if (!cancelled) setError(err.message);
      }
    }

    loadCountrySeries();
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
      setClubLoading(false);
      setClubError("");
      return;
    }
    let cancelled = false;
    setClubLoading(true);
    setClubDetail(null);
    setClubError("");
    (async () => {
      try {
        const data = await fetchClubDetailWithFallbacks(parsed.teamId, 240000);
        if (!cancelled) {
          setClubDetail(data);
          setClubLoading(false);
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
  }, [selectedTeamId]);

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
      marker: { line: { color: "#ffffff", width: 0.6 } },
      colorbar: { title: "Best team<br>rating", titleside: "right" },
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
        line: { color: "#ffffff", width: 1.2 },
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
      title: {
              text: "Hover for summary, click country for full page",
        font: { color: THEME.text, size: 15 },
      },
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
        lataxis: { range: [34, 71] },
        lonaxis: { range: [-12, 45] },
        domain: { x: [0, 1], y: [0, 1] },
      },
      margin: { l: 4, r: 96, t: 42, b: 6 },
    }),
    [mapDims.w, mapDims.h]
  );

  const countryPlotLayout = {
    title: {
      text: "Average and Top Ratings Over Time",
      font: { color: THEME.text },
    },
    font: { color: THEME.text },
    paper_bgcolor: THEME.plotPaper,
    plot_bgcolor: THEME.plotGrid,
    xaxis: {
      title: "Date",
      gridcolor: "#dde3eb",
      zerolinecolor: "#dde3eb",
      color: THEME.muted,
    },
    yaxis: {
      title: "Glicko-2 Rating",
      gridcolor: "#dde3eb",
      zerolinecolor: "#dde3eb",
      color: THEME.muted,
    },
    margin: { l: 50, r: 20, t: 50, b: 40 },
  };

  const teamPlotLayout = {
    title: {
      text: "Weekly Team Rating Movement",
      font: { color: THEME.text },
    },
    font: { color: THEME.text },
    paper_bgcolor: THEME.plotPaper,
    plot_bgcolor: THEME.plotGrid,
    xaxis: {
      title: "Date",
      gridcolor: "#dde3eb",
      zerolinecolor: "#dde3eb",
      color: THEME.muted,
    },
    yaxis: {
      title: "Glicko-2 Rating",
      gridcolor: "#dde3eb",
      zerolinecolor: "#dde3eb",
      color: THEME.muted,
    },
    margin: { l: 50, r: 20, t: 50, b: 40 },
  };

  const teamTrendData = [
    {
      x: teamSeries.map((d) => d.week_date),
      y: teamSeries.map((d) => d.rating),
      mode: "lines+markers",
      type: "scatter",
      name: selectedTeam ? selectedTeam.team_name : "Team",
      line: { color: THEME.primary, width: 2 },
    },
  ];

  const countryTrendData = [
    {
      x: countrySeries.map((d) => d.week_date),
      y: countrySeries.map((d) => d.average_rating),
      mode: "lines",
      type: "scatter",
      name: "Average Rating",
      line: { color: THEME.success, width: 2 },
    },
    {
      x: countrySeries.map((d) => d.week_date),
      y: countrySeries.map((d) => d.top_rating),
      mode: "lines",
      type: "scatter",
      name: "Top Team Rating",
      line: { color: THEME.accent, width: 2 },
    },
  ];

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
    <div className="container">
      {error && route.page !== "club" && <div className="card error">{error}</div>}

      {route.page === "club" ? (
        <>
          <nav className="page-nav" aria-label="Breadcrumb">
            <a
              href="#/"
              onClick={(e) => {
                e.preventDefault();
                navigate("/");
              }}
            >
              ← Map & rankings
            </a>
            {clubCountrySlug ? (
              <>
                {" · "}
                <a
                  href={`#/country/${encodeURIComponent(clubCountrySlug)}`}
                  onClick={(e) => {
                    e.preventDefault();
                    navigate(`/country/${encodeURIComponent(clubCountrySlug)}`);
                  }}
                >
                  ← {formatCountryDisplay(clubCountrySlug)}
                </a>
              </>
            ) : null}
          </nav>

          {clubLoading || (loading && !clubDetail && !clubError) ? (
            <div className="card">
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
                {formatCountryDisplay(clubDetail.country_name)} · Full match history and strongest weekly
                rating moves (Glicko-2 updates are applied per rating week, not always per match date).
              </p>

              <div className="card">
                <h2>Rating over time</h2>
                <Plot data={teamTrendData} layout={teamPlotLayout} />
              </div>

              <div className="club-extremes-grid">
                <div className="card">
                  <h2>Largest weekly gains</h2>
                  <p className="small" style={{ marginTop: "-8px" }}>
                    Weeks with the biggest positive rating_change in the model.
                  </p>
                  <div className="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Week</th>
                          <th>Week start</th>
                          <th>Rating</th>
                          <th>Δ rating</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(clubDetail.weekly_gains || []).map((row, i) => (
                          <tr key={`gain-${row.week}-${i}`}>
                            <td>{row.week}</td>
                            <td>{row.week_date}</td>
                            <td>{Number(row.rating).toFixed(1)}</td>
                            <td className="numeric-pos">{formatSignedRating(row.rating_change)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div className="card">
                  <h2>Largest weekly losses</h2>
                  <p className="small" style={{ marginTop: "-8px" }}>
                    Weeks with the most negative rating_change.
                  </p>
                  <div className="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Week</th>
                          <th>Week start</th>
                          <th>Rating</th>
                          <th>Δ rating</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(clubDetail.weekly_losses || []).map((row, i) => (
                          <tr key={`loss-${row.week}-${i}`}>
                            <td>{row.week}</td>
                            <td>{row.week_date}</td>
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
                          <td>{row.competition}</td>
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
            <div className="card">Loading country…</div>
          ) : !countrySlugKnown ? (
            <div className="card error">
              Unknown country “{formatCountryDisplay(route.country)}”.{" "}
              <a
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
                Ratings summary for this nation. Choose a team for movement charts and notable matches.
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

              <div className="card controls">
                <div>
                  <label>Team</label>
                  <select value={selectedTeamId} onChange={(e) => setSelectedTeamId(e.target.value)}>
                    {filteredTeams.map((team) => (
                      <option key={team.pid} value={team.pid}>
                        {team.team_name}
                      </option>
                    ))}
                  </select>
                </div>
                {selectedTeamId ? (
                  <div style={{ alignSelf: "end" }}>
                    <a
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
                <h2>Country movement</h2>
                <Plot data={countryTrendData} layout={countryPlotLayout} />
              </div>

              <div className="card">
                <h2>Team movement: {selectedTeam ? selectedTeam.team_name : "Team"}</h2>
                <Plot data={teamTrendData} layout={teamPlotLayout} />
              </div>

              <div className="card">
                <h2>Biggest upsets</h2>
                <table>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Match</th>
                      <th>Competition</th>
                      <th>Upset magnitude</th>
                    </tr>
                  </thead>
                  <tbody>
                    {biggestMatches.upsets.map((row, index) => (
                      <tr key={`upset-${index}`}>
                        <td>{String(row.match_date).slice(0, 10)}</td>
                        <td>{formatMatch(row)}</td>
                        <td>{row.competition}</td>
                        <td>{row.upset_magnitude.toFixed(3)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="card">
                <h2>Largest rating swings</h2>
                <table>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Match</th>
                      <th>Competition</th>
                      <th>Total swing</th>
                    </tr>
                  </thead>
                  <tbody>
                    {biggestMatches.swings.map((row, index) => (
                      <tr key={`swing-${index}`}>
                        <td>{String(row.match_date).slice(0, 10)}</td>
                        <td>{formatMatch(row)}</td>
                        <td>{row.competition}</td>
                        <td>{row.absolute_rating_swing.toFixed(2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </>
      ) : (
        <>
          <h1>European Football Glicko-2 Dashboard</h1>
          <p className="small">
            Hover for snapshots; <strong>click a country</strong> on the map to open its summary page.
          </p>

          <section className="map-full-width" aria-label="European ratings map">
            <div className="card">
              <h2>European Ratings Map</h2>
              <p className="small" style={{ marginTop: "-6px", marginBottom: "12px" }}>
                Country shading uses each nation&apos;s <strong>best</strong> club rating (light blue = lower, dark blue = higher).
              </p>
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
              <p className="small">
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

          {loading && <div className="card">Loading data…</div>}

          <div className="card">
            <h2>Current Top 25 (Latest Week)</h2>
            <table>
              <thead>
                <tr>
                  <th>Team</th>
                  <th>Country</th>
                  <th>Rating</th>
                  <th>RD</th>
                </tr>
              </thead>
              <tbody>
                {topSnapshot.map((row) => (
                  <tr key={row.pid}>
                    <td>{row.team_name}</td>
                    <td>{formatCountryDisplay(row.country_name)}</td>
                    <td>{row.rating.toFixed(1)}</td>
                    <td>{row.rd.toFixed(1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<App />);
