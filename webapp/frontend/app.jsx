const { useEffect, useMemo, useState } = React;

async function getJson(url, options = {}) {
  const { allow404 = false } = options;
  const response = await fetch(url);
  if (allow404 && response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function Plot({ data, layout, config, onClick, onHover }) {
  const ref = React.useRef(null);

  useEffect(() => {
    if (!ref.current) return;
    Plotly.newPlot(ref.current, data, layout, {
      responsive: true,
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
      Plotly.purge(ref.current);
    };
  }, [data, layout, config, onClick, onHover]);

  return <div ref={ref} style={{ width: "100%", minHeight: 420 }} />;
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
        if (countriesData.length > 0) setSelectedCountry(countriesData[0]);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    }
    init();
  }, []);

  useEffect(() => {
    if (!selectedCountry) return;
    async function loadCountrySeries() {
      try {
        const [countryData, countryTeams] = await Promise.all([
          getJson(`/api/country/${selectedCountry}/timeseries`),
          getJson(`/api/teams?country=${selectedCountry}`),
        ]);
        setCountrySeries(countryData);
        if (countryTeams.length > 0) {
          setSelectedTeamId(String(countryTeams[0].pid));
        } else {
          setSelectedTeamId("");
        }
      } catch (err) {
        setError(err.message);
      }
    }
    loadCountrySeries();
  }, [selectedCountry]);

  useEffect(() => {
    if (!selectedTeamId) return;
    async function loadTeamDetail() {
      try {
        const [series, matches] = await Promise.all([
          getJson(`/api/team/${selectedTeamId}/timeseries`),
          getJson(`/api/team/${selectedTeamId}/biggest-matches?limit=12`, { allow404: true }),
        ]);
        setTeamSeries(series);
        setBiggestMatches(matches || { upsets: [], swings: [] });
      } catch (err) {
        setError(err.message);
      }
    }
    loadTeamDetail();
  }, [selectedTeamId]);

  const filteredTeams = useMemo(() => {
    if (!selectedCountry) return teams;
    return teams.filter((team) => team.country_name.toLowerCase() === selectedCountry.toLowerCase());
  }, [teams, selectedCountry]);

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

  const minAverage = mapPoints.length > 0 ? Math.min(...mapPoints.map((p) => p.average_rating)) : 0;
  const maxAverage = mapPoints.length > 0 ? Math.max(...mapPoints.map((p) => p.average_rating)) : 1;

  const markerSizes = mapPoints.map((p) => {
    if (maxAverage === minAverage) return 16;
    return 12 + ((p.average_rating - minAverage) / (maxAverage - minAverage)) * 16;
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
      z: choroplethPoints.map((p) => p.average_rating),
      customdata: choroplethPoints.map((p) => [
        p.country_name,
        p.average_rating,
        p.active_teams,
        p.top_team_name,
        p.top_team_rating,
      ]),
      hovertemplate:
        "<b>%{location}</b><br>" +
        "Avg Rating: %{customdata[1]:.1f}<br>" +
        "Active Teams: %{customdata[2]}<br>" +
        "Top Team: %{customdata[3]} (%{customdata[4]:.1f})<extra></extra>",
      colorscale: "Viridis",
      zmin: minAverage,
      zmax: maxAverage,
      marker: { line: { color: "#ffffff", width: 0.6 } },
      colorbar: { title: "Avg Rating" },
    },
    {
      type: "scattergeo",
      mode: "markers+text",
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
        "Avg Rating: %{customdata[1]:.1f}<br>" +
        "Active Teams: %{customdata[2]}<br>" +
        "Top Team: %{customdata[3]} (%{customdata[4]:.1f})<extra></extra>",
      marker: {
        size: markerOnlyPoints.map((p) => {
          const idx = mapPoints.findIndex((m) => m.country_name === p.country_name);
          return idx >= 0 ? markerSizes[idx] : 14;
        }),
        color: markerOnlyPoints.map((p) => p.average_rating),
        colorscale: "Viridis",
        cmin: minAverage,
        cmax: maxAverage,
        line: { color: "#ffffff", width: 1.2 },
        opacity: 0.95,
      },
    },
  ];

  const teamTrendData = [
    {
      x: teamSeries.map((d) => d.week_date),
      y: teamSeries.map((d) => d.rating),
      mode: "lines+markers",
      type: "scatter",
      name: selectedTeam ? selectedTeam.team_name : "Team",
      line: { color: "#1d4ed8" },
    },
  ];

  const countryTrendData = [
    {
      x: countrySeries.map((d) => d.week_date),
      y: countrySeries.map((d) => d.average_rating),
      mode: "lines",
      type: "scatter",
      name: "Average Rating",
      line: { color: "#16a34a" },
    },
    {
      x: countrySeries.map((d) => d.week_date),
      y: countrySeries.map((d) => d.top_rating),
      mode: "lines",
      type: "scatter",
      name: "Top Team Rating",
      line: { color: "#f59e0b" },
    },
  ];

  return (
    <div className="container">
      <h1>European Football Glicko-2 Dashboard</h1>
      <p className="small">
        Hover a nation for a quick summary, then click to open detailed country and team views.
      </p>

      {error && <div className="card error">{error}</div>}

      <div className="card">
        <h2>European Ratings Map</h2>
        <Plot
          data={mapData}
          layout={{
            title: "Hover for summary, click for details",
            geo: {
              scope: "europe",
              projection: { type: "mercator" },
              showland: true,
              landcolor: "#ecf3f7",
              showcountries: true,
              countrycolor: "#b8c2cc",
              showocean: true,
              oceancolor: "#dbeafe",
              lataxis: { range: [34, 71] },
              lonaxis: { range: [-12, 45] },
            },
            margin: { l: 20, r: 20, t: 50, b: 20 },
          }}
          onClick={(event) => {
            const point = event.points?.[0];
            if (!point || !point.customdata) return;
            setSelectedCountry(point.customdata[0]);
          }}
          onHover={(event) => {
            const point = event.points?.[0];
            if (!point || !point.customdata) return;
            setHoveredCountry(point.customdata[0]);
          }}
        />
        <p className="small">
          {hoveredCountrySummary
            ? `${hoveredCountrySummary.country_name.toUpperCase()}: avg ${hoveredCountrySummary.average_rating.toFixed(
                1
              )}, ${hoveredCountrySummary.active_teams} teams, top team ${hoveredCountrySummary.top_team_name}.`
            : "Hover any marker to preview that nation."}
        </p>
      </div>

      <div className="card controls">
        <div>
          <label>Country</label>
          <select value={selectedCountry} onChange={(e) => setSelectedCountry(e.target.value)}>
            {countries.map((country) => (
              <option key={country} value={country}>
                {country}
              </option>
            ))}
          </select>
        </div>

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
      </div>

      {loading && <div className="card">Loading data...</div>}

      {selectedCountrySummary && (
        <div className="card">
          <h2>{selectedCountrySummary.country_name.toUpperCase()} Snapshot</h2>
          <p className="small">
            Average rating {selectedCountrySummary.average_rating.toFixed(1)} across{" "}
            {selectedCountrySummary.active_teams} active teams. Top side:{" "}
            {selectedCountrySummary.top_team_name} ({selectedCountrySummary.top_team_rating.toFixed(1)}).
          </p>
        </div>
      )}

      <div className="card">
        <h2>Country Movement: {selectedCountry}</h2>
        <Plot
          data={countryTrendData}
          layout={{
            title: "Average and Top Ratings Over Time",
            xaxis: { title: "Date" },
            yaxis: { title: "Glicko-2 Rating" },
            margin: { l: 50, r: 20, t: 50, b: 40 },
          }}
        />
      </div>

      <div className="card">
        <h2>Team Movement: {selectedTeam ? selectedTeam.team_name : "Team"}</h2>
        <Plot
          data={teamTrendData}
          layout={{
            title: "Weekly Team Rating Movement",
            xaxis: { title: "Date" },
            yaxis: { title: "Glicko-2 Rating" },
            margin: { l: 50, r: 20, t: 50, b: 40 },
          }}
        />
      </div>

      <div className="card">
        <h2>Biggest Upsets</h2>
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Match</th>
              <th>Competition</th>
              <th>Upset Magnitude</th>
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
        <h2>Largest Rating Swings</h2>
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Match</th>
              <th>Competition</th>
              <th>Total Swing</th>
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
                <td>{row.country_name}</td>
                <td>{row.rating.toFixed(1)}</td>
                <td>{row.rd.toFixed(1)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<App />);
