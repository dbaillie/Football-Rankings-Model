from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote

import requests
from bs4 import BeautifulSoup

country = 'switzerland'
DEFAULT_PAGE = f"https://www.football-data.co.uk/{country}.php"
DEFAULT_OUTPUT_ROOT = Path(f"data/football/")


def safe_get(url: str, timeout: int = 30) -> requests.Response:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response


def slug_from_page_url(page_url: str) -> str:
    """
    Turn:
      https://www.football-data.co.uk/englandm.php -> englandm
      https://www.football-data.co.uk/denmark.php  -> denmark
    """
    name = Path(urlparse(page_url).path).name
    return name.replace(".php", "").lower()


def extract_csv_links(page_url: str) -> list[dict]:
    """
    Extract every CSV link from a football-data country page.

    Handles:
      1) standard season-style URLs like /mmz4281/2526/E0.csv
      2) simpler/fallback CSV links that do not match that exact pattern

    Returns dicts with:
      - page_slug
      - season_code (or None)
      - league_code (or inferred stem)
      - url
      - filename
      - subdir
    """
    html = safe_get(page_url).text
    soup = BeautifulSoup(html, "html.parser")

    page_slug = slug_from_page_url(page_url)
    results: list[dict] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        full_url = urljoin(page_url, href)

        if not full_url.lower().endswith(".csv"):
            continue

        if full_url in seen:
            continue
        seen.add(full_url)

        parsed_path = unquote(urlparse(full_url).path)
        stem = Path(parsed_path).stem
        filename = Path(parsed_path).name

        # Standard football-data pattern:
        # /mmz4281/2526/E0.csv
        std_match = re.search(
            r"/mmz4281/(\d{4})/([A-Za-z0-9_-]+)\.csv$",
            parsed_path,
            flags=re.IGNORECASE,
        )

        if std_match:
            season_code = std_match.group(1)
            league_code = std_match.group(2).upper()
            filename = f"{league_code}.csv"
            subdir = season_code
        else:
            # Fallback for pages like denmark.php or any other non-standard structure:
            # try to infer season from anywhere in the path, otherwise keep all in a "misc" folder
            season_guess = re.search(r"/(\d{4})/", parsed_path)
            season_code = season_guess.group(1) if season_guess else None

            league_code = re.sub(r"[^A-Za-z0-9_-]+", "_", stem).strip("_") or "data"
            subdir = season_code if season_code else "misc"

        results.append(
            {
                "page_slug": page_slug,
                "season_code": season_code,
                "league_code": league_code,
                "url": full_url,
                "filename": filename,
                "subdir": subdir,
            }
        )

    return sorted(
        results,
        key=lambda x: (
            x["subdir"],
            x["league_code"],
            x["filename"],
        ),
        reverse=True,
    )


def download_file(url: str, out_path: Path, chunk_size: int = 1024 * 1024) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)


def download_page_csvs(page_url: str, output_root: Path = DEFAULT_OUTPUT_ROOT) -> None:
    page_slug = slug_from_page_url(page_url)
    csv_links = extract_csv_links(page_url)

    print(f"Page: {page_url}")
    print(f"Detected page slug: {page_slug}")
    print(f"Found {len(csv_links)} CSV link(s)")

    if not csv_links:
        raise RuntimeError("No CSV links found. The page structure may have changed.")

    for item in csv_links:
        out_dir = output_root / page_slug / item["subdir"]
        out_path = out_dir / item["filename"]

        try:
            print(f"Downloading {item['url']} -> {out_path}")
            download_file(item["url"], out_path)
        except Exception as e:
            print(f"Failed: {item['url']} ({e})")

    print("Done.")


if __name__ == "__main__":
    # Change this to Denmark, England, etc.
    PAGE_URL = f"https://www.football-data.co.uk/{country}.php"
    download_page_csvs(PAGE_URL)