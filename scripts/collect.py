#!/usr/bin/env python3
"""
GitHub Repository Analytics Collector

Collects metrics from GitHub API for configured repositories and stores
them as daily JSON snapshots and CSV aggregates.
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import requests

# Configuration
SCRIPT_DIR = Path(__file__).parent
ROOT_DIR = SCRIPT_DIR.parent
CONFIG_FILE = ROOT_DIR / "config.json"
DATA_DIR = ROOT_DIR / "data"

# GitHub API
GITHUB_API_BASE = "https://api.github.com"
GITHUB_TOKEN = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")

# CSV Headers
AGGREGATE_HEADERS = [
    "date", "views", "views_unique", "clones", "clones_unique",
    "stars", "forks", "open_issues", "open_prs", "watchers", "size_kb", "releases_downloads"
]
RELEASES_HEADERS = [
    "date", "tag", "asset_name", "asset_size", "download_count", "downloads_delta"
]
STAR_HISTORY_HEADERS = ["date", "stars"]
PACKAGES_HEADERS = ["date", "source", "package", "daily_downloads", "weekly_downloads"]

# Package Registry APIs
NPM_API_BASE = "https://api.npmjs.org"


def get_headers() -> dict[str, str]:
    """Get headers for GitHub API requests."""
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return headers


def api_get(endpoint: str) -> dict[str, Any] | list | None:
    """Make a GET request to the GitHub API."""
    url = f"{GITHUB_API_BASE}{endpoint}"
    try:
        response = requests.get(url, headers=get_headers(), timeout=30)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 202:
            # Statistics are being computed, return None
            print(f"  ⏳ {endpoint} - statistics being computed (202)")
            return None
        elif response.status_code == 403:
            print(f"  ⚠️  {endpoint} - access denied (403)")
            return None
        elif response.status_code == 404:
            print(f"  ⚠️  {endpoint} - not found (404)")
            return None
        else:
            print(f"  ❌ {endpoint} - HTTP {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"  ❌ {endpoint} - {e}")
        return None


def api_get_paginated(endpoint: str, max_pages: int = 10) -> list:
    """Get paginated results from GitHub API."""
    results = []
    page = 1
    while page <= max_pages:
        url = f"{GITHUB_API_BASE}{endpoint}"
        separator = "&" if "?" in endpoint else "?"
        url = f"{url}{separator}per_page=100&page={page}"
        
        try:
            response = requests.get(url, headers=get_headers(), timeout=30)
            if response.status_code != 200:
                break
            data = response.json()
            if not data:
                break
            results.extend(data)
            if len(data) < 100:
                break
            page += 1
        except requests.RequestException:
            break
    
    return results


def collect_repo_info(owner: str, repo: str) -> dict[str, Any] | None:
    """Collect basic repository information."""
    data = api_get(f"/repos/{owner}/{repo}")
    if data:
        return {
            "full_name": data.get("full_name"),
            "stars": data.get("stargazers_count", 0),
            "forks": data.get("forks_count", 0),
            "watchers": data.get("subscribers_count", 0),  # subscribers = watchers
            "open_issues_and_prs": data.get("open_issues_count", 0),  # Note: includes PRs
            "size_kb": data.get("size", 0),
        }
    return None


def collect_issue_counts(owner: str, repo: str) -> dict[str, int]:
    """Collect actual issue and PR counts separately using search API."""
    counts = {"open_issues": 0, "open_prs": 0}
    
    # Get open issues count (excluding PRs)
    issues_url = f"{GITHUB_API_BASE}/search/issues?q=repo:{owner}/{repo}+type:issue+state:open"
    try:
        response = requests.get(issues_url, headers=get_headers(), timeout=30)
        if response.status_code == 200:
            counts["open_issues"] = response.json().get("total_count", 0)
    except requests.RequestException:
        pass
    
    # Get open PRs count
    prs_url = f"{GITHUB_API_BASE}/search/issues?q=repo:{owner}/{repo}+type:pr+state:open"
    try:
        response = requests.get(prs_url, headers=get_headers(), timeout=30)
        if response.status_code == 200:
            counts["open_prs"] = response.json().get("total_count", 0)
    except requests.RequestException:
        pass
    
    return counts


def collect_traffic(owner: str, repo: str) -> dict[str, Any]:
    """Collect traffic data (requires admin access)."""
    traffic = {}
    
    # Views
    views_data = api_get(f"/repos/{owner}/{repo}/traffic/views")
    if views_data:
        traffic["views"] = {
            "count": views_data.get("count", 0),
            "uniques": views_data.get("uniques", 0),
            "daily": views_data.get("views", [])
        }
    
    # Clones
    clones_data = api_get(f"/repos/{owner}/{repo}/traffic/clones")
    if clones_data:
        traffic["clones"] = {
            "count": clones_data.get("count", 0),
            "uniques": clones_data.get("uniques", 0),
            "daily": clones_data.get("clones", [])
        }
    
    # Referrers
    referrers = api_get(f"/repos/{owner}/{repo}/traffic/popular/referrers")
    if referrers:
        traffic["referrers"] = referrers
    
    # Popular paths
    paths = api_get(f"/repos/{owner}/{repo}/traffic/popular/paths")
    if paths:
        traffic["popular_paths"] = paths
    
    return traffic


def collect_releases(owner: str, repo: str) -> list[dict[str, Any]]:
    """Collect all releases with download counts."""
    releases_data = api_get_paginated(f"/repos/{owner}/{repo}/releases")
    releases = []
    
    for release in releases_data:
        assets = []
        for asset in release.get("assets", []):
            assets.append({
                "name": asset.get("name"),
                "download_count": asset.get("download_count", 0),
                "size": asset.get("size", 0),
            })
        
        releases.append({
            "tag": release.get("tag_name"),
            "name": release.get("name"),
            "published_at": release.get("published_at"),
            "assets": assets,
        })
    
    return releases


def collect_languages(owner: str, repo: str) -> dict[str, int]:
    """Collect language breakdown."""
    data = api_get(f"/repos/{owner}/{repo}/languages")
    return data if data else {}


def collect_code_frequency(owner: str, repo: str) -> dict[str, Any]:
    """Collect code frequency (additions/deletions per week)."""
    data = api_get(f"/repos/{owner}/{repo}/stats/code_frequency")
    if data and len(data) > 0:
        # Return the most recent week
        latest = data[-1]
        return {
            "week": datetime.fromtimestamp(latest[0], tz=timezone.utc).strftime("%Y-%m-%d"),
            "additions": latest[1],
            "deletions": abs(latest[2]),
        }
    return {}


def collect_contributors_count(owner: str, repo: str) -> int:
    """Get the number of contributors."""
    # Use per_page=1 and check the Link header for total count
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contributors?per_page=1&anon=true"
    try:
        response = requests.get(url, headers=get_headers(), timeout=30)
        if response.status_code == 200:
            # Check Link header for last page number
            link_header = response.headers.get("Link", "")
            if 'rel="last"' in link_header:
                # Parse the last page number
                for part in link_header.split(","):
                    if 'rel="last"' in part:
                        # Extract page number from URL
                        import re
                        match = re.search(r'[?&]page=(\d+)', part)
                        if match:
                            return int(match.group(1))
            # If no Link header, count the results
            return len(response.json())
    except requests.RequestException:
        pass
    return 0


def collect_star_history(owner: str, repo: str, sample_count: int = 15) -> list[dict[str, Any]]:
    """
    Reconstruct star history by sampling stargazer pages.
    Uses the starred_at timestamp from the stargazers API.
    Limited to 40,000 stars (400 pages max).
    """
    import re
    
    headers = get_headers()
    headers["Accept"] = "application/vnd.github.star+json"
    
    base_url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/stargazers"
    
    try:
        # Get first page to determine total pages
        response = requests.get(f"{base_url}?per_page=100", headers=headers, timeout=30)
        if response.status_code != 200:
            print(f"  ⚠️  Star history - HTTP {response.status_code}")
            return []
        
        # Parse Link header for page count
        link_header = response.headers.get("Link", "")
        page_count = 1
        
        match = re.search(r'[&?]page=(\d+)>; rel="last"', link_header)
        if match:
            page_count = int(match.group(1))
        
        # Cap at 400 pages (40k stars limit)
        if page_count > 400:
            print(f"  ⚠️  Star history - capped at 40,000 stars (repo has {page_count * 100}+)")
            page_count = 400
        
        # Determine which pages to sample
        if page_count <= sample_count:
            sample_pages = list(range(1, page_count + 1))
        else:
            sample_pages = [
                max(1, round((i * page_count) / sample_count))
                for i in range(1, sample_count + 1)
            ]
            if 1 not in sample_pages:
                sample_pages[0] = 1
        
        star_records = []
        
        # First page already fetched
        first_data = response.json()
        if first_data and 1 in sample_pages:
            star_records.append({
                "date": first_data[0]["starred_at"][:10],
                "stars": 1
            })
            sample_pages.remove(1)
        
        # Fetch remaining sample pages
        for page in sample_pages:
            resp = requests.get(f"{base_url}?per_page=100&page={page}", headers=headers, timeout=30)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if data:
                star_records.append({
                    "date": data[0]["starred_at"][:10],
                    "stars": (page - 1) * 100 + 1
                })
        
        # Sort by date
        star_records.sort(key=lambda x: x["date"])
        
        return star_records
        
    except requests.RequestException as e:
        print(f"  ❌ Star history - {e}")
        return []


# =============================================================================
# Package Registry Collection (npm, etc.)
# =============================================================================

def collect_npm_downloads(package_name: str, date: str) -> dict[str, Any]:
    """
    Collect npm download stats for a package.
    Returns daily downloads for the specified date and total downloads.
    """
    result = {
        "package": package_name,
        "source": "npm",
        "daily_downloads": 0,
        "weekly_downloads": 0,
    }
    
    try:
        # Get daily downloads for the specific date
        daily_url = f"{NPM_API_BASE}/downloads/point/{date}/{package_name}"
        response = requests.get(daily_url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            result["daily_downloads"] = data.get("downloads", 0)
        
        # Get weekly downloads (last 7 days)
        weekly_url = f"{NPM_API_BASE}/downloads/point/last-week/{package_name}"
        response = requests.get(weekly_url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            result["weekly_downloads"] = data.get("downloads", 0)
            
    except requests.RequestException as e:
        print(f"  ⚠️  npm {package_name} - {e}")
    
    return result


def collect_npm_history(package_name: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    """
    Collect historical npm download data for a date range.
    npm API provides up to 18 months of historical data.
    Returns list of {date, package, source, daily_downloads, weekly_downloads}
    """
    records = []
    
    try:
        # Fetch historical daily downloads
        url = f"{NPM_API_BASE}/downloads/range/{start_date}:{end_date}/{package_name}"
        response = requests.get(url, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            downloads_list = data.get("downloads", [])
            
            # Build records from daily data
            for entry in downloads_list:
                records.append({
                    "date": entry["day"],
                    "package": package_name,
                    "source": "npm",
                    "daily_downloads": entry["downloads"],
                    "weekly_downloads": 0,  # Will calculate below
                })
            
            # Calculate rolling 7-day weekly downloads for each date
            for i, record in enumerate(records):
                # Sum the previous 7 days (including current day)
                start_idx = max(0, i - 6)
                weekly_sum = sum(r["daily_downloads"] for r in records[start_idx:i+1])
                record["weekly_downloads"] = weekly_sum
                
        elif response.status_code == 404:
            print(f"  ⚠️  npm {package_name} - package not found or no data")
        else:
            print(f"  ⚠️  npm {package_name} history - HTTP {response.status_code}")
            
    except requests.RequestException as e:
        print(f"  ⚠️  npm {package_name} history - {e}")
    
    return records


def collect_package_history(packages_config: dict[str, list[str]], existing_dates: set[str]) -> list[dict[str, Any]]:
    """
    Collect historical package download data, skipping dates we already have.
    Uses npm API's ability to return historical data (up to 18 months).
    """
    # Determine date range: from 1 year ago to yesterday
    end_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")
    
    all_records = []
    
    for package_name in packages_config.get("npm", []):
        print(f"  📦 Fetching npm history for {package_name} ({start_date} to {end_date})...")
        records = collect_npm_history(package_name, start_date, end_date)
        
        # Filter out dates we already have
        new_records = [r for r in records if r["date"] not in existing_dates]
        
        if new_records:
            print(f"     Found {len(new_records)} new days of data")
            all_records.extend(new_records)
        else:
            print(f"     All historical data already collected")
    
    return all_records


def collect_package_downloads(packages_config: dict[str, list[str]], date: str) -> list[dict[str, Any]]:
    """
    Collect download stats from all configured package registries.
    packages_config format: {"npm": ["package1", "package2"], "pypi": ["pkg1"]}
    """
    results = []
    
    # NPM packages
    for package_name in packages_config.get("npm", []):
        print(f"  📦 Collecting npm stats for {package_name}...")
        stats = collect_npm_downloads(package_name, date)
        if stats["daily_downloads"] > 0 or stats["weekly_downloads"] > 0:
            results.append(stats)
            print(f"     Daily: {stats['daily_downloads']:,}, Weekly: {stats['weekly_downloads']:,}")
        else:
            print(f"     No download data available")
    
    # Future: Add PyPI, NuGet, etc.
    # for package_name in packages_config.get("pypi", []):
    #     results.append(collect_pypi_downloads(package_name, date))
    
    return results


def calculate_total_downloads(releases: list[dict[str, Any]]) -> int:
    """Calculate total downloads across all releases."""
    total = 0
    for release in releases:
        for asset in release.get("assets", []):
            total += asset.get("download_count", 0)
    return total


def get_previous_releases_data(repo_data_dir: Path) -> dict[str, int]:
    """Load previous releases data to calculate deltas."""
    releases_csv = repo_data_dir / "releases.csv"
    if not releases_csv.exists():
        return {}
    
    # Get the most recent date's data
    previous = {}
    try:
        with open(releases_csv, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = f"{row['tag']}|{row['asset_name']}"
                previous[key] = int(row.get("download_count", 0))
    except (IOError, ValueError):
        pass
    
    return previous


def ensure_csv_headers(csv_path: Path, headers: list[str]) -> None:
    """Ensure CSV file exists with headers."""
    if not csv_path.exists():
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)


def date_exists_in_csv(csv_path: Path, date: str) -> bool:
    """Check if a date already exists in the CSV file."""
    if not csv_path.exists():
        return False
    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("date") == date:
                    return True
    except (IOError, ValueError):
        pass
    return False


def append_aggregate_row(csv_path: Path, date: str, snapshot: dict[str, Any]) -> bool:
    """Append a row to the aggregate CSV. Returns False if date already exists."""
    ensure_csv_headers(csv_path, AGGREGATE_HEADERS)
    
    if date_exists_in_csv(csv_path, date):
        return False
    
    repo_info = snapshot.get("repo", {})
    traffic = snapshot.get("traffic", {})
    views = traffic.get("views", {})
    clones = traffic.get("clones", {})
    issue_counts = snapshot.get("issue_counts", {})
    
    row = [
        date,
        views.get("count", ""),
        views.get("uniques", ""),
        clones.get("count", ""),
        clones.get("uniques", ""),
        repo_info.get("stars", 0),
        repo_info.get("forks", 0),
        issue_counts.get("open_issues", 0),
        issue_counts.get("open_prs", 0),
        repo_info.get("watchers", 0),
        repo_info.get("size_kb", 0),
        snapshot.get("releases_downloads", 0),
    ]
    
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)
    return True


def update_releases_csv(csv_path: Path, date: str, releases: list[dict[str, Any]], 
                        previous_data: dict[str, int]) -> bool:
    """Update releases CSV with current download counts. Returns False if date already exists."""
    ensure_csv_headers(csv_path, RELEASES_HEADERS)
    
    if date_exists_in_csv(csv_path, date):
        return False
    
    rows = []
    for release in releases:
        tag = release.get("tag", "")
        for asset in release.get("assets", []):
            name = asset.get("name", "")
            count = asset.get("download_count", 0)
            size = asset.get("size", 0)
            
            key = f"{tag}|{name}"
            previous_count = previous_data.get(key, count)  # Default to current if new
            delta = count - previous_count
            
            rows.append([date, tag, name, size, count, delta])
    
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    return True


def update_packages_csv(csv_path: Path, date: str, packages_data: list[dict[str, Any]]) -> bool:
    """Update packages CSV with download counts. Returns False if date already exists."""
    ensure_csv_headers(csv_path, PACKAGES_HEADERS)
    
    if date_exists_in_csv(csv_path, date):
        return False
    
    rows = []
    for pkg in packages_data:
        rows.append([
            date,
            pkg.get("source", ""),
            pkg.get("package", ""),
            pkg.get("daily_downloads", 0),
            pkg.get("weekly_downloads", 0),
        ])
    
    if rows:
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)
    return True


def collect_repo(full_name: str, date: str, packages_config: dict[str, list[str]] = None) -> bool:
    """Collect all metrics for a single repository."""
    owner, repo = full_name.split("/")
    print(f"\n📊 Collecting metrics for {full_name}...")
    
    # Set up data directory
    repo_data_dir = DATA_DIR / owner / repo
    snapshots_dir = repo_data_dir / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if we already collected today
    snapshot_file = snapshots_dir / f"{date}.json"
    if snapshot_file.exists():
        print(f"  ⏭️  Already collected for {date}, skipping")
        return True
    
    # Collect all metrics
    repo_info = collect_repo_info(owner, repo)
    if not repo_info:
        print(f"  ❌ Failed to fetch repository info")
        return False
    
    traffic = collect_traffic(owner, repo)
    releases = collect_releases(owner, repo)
    languages = collect_languages(owner, repo)
    code_frequency = collect_code_frequency(owner, repo)
    contributors_count = collect_contributors_count(owner, repo)
    total_downloads = calculate_total_downloads(releases)
    star_history = collect_star_history(owner, repo)
    issue_counts = collect_issue_counts(owner, repo)
    
    # Collect package downloads if configured
    packages_data = []
    if packages_config:
        packages_data = collect_package_downloads(packages_config, date)
    
    # Build snapshot
    snapshot = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "repo": repo_info,
        "traffic": traffic,
        "releases": releases,
        "languages": languages,
        "code_frequency": code_frequency,
        "contributors_count": contributors_count,
        "releases_downloads": total_downloads,
        "star_history": star_history,
        "issue_counts": issue_counts,
        "packages": packages_data,
    }
    
    # Save snapshot
    with open(snapshot_file, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)
    print(f"  ✅ Saved snapshot to {snapshot_file.relative_to(ROOT_DIR)}")
    
    # Update aggregate CSV
    aggregate_csv = repo_data_dir / "aggregate.csv"
    if append_aggregate_row(aggregate_csv, date, snapshot):
        print(f"  ✅ Updated {aggregate_csv.relative_to(ROOT_DIR)}")
    else:
        print(f"  ⏭️  {aggregate_csv.relative_to(ROOT_DIR)} already has data for {date}")
    
    # Update releases CSV
    releases_csv = repo_data_dir / "releases.csv"
    previous_releases = get_previous_releases_data(repo_data_dir)
    if update_releases_csv(releases_csv, date, releases, previous_releases):
        print(f"  ✅ Updated {releases_csv.relative_to(ROOT_DIR)}")
    else:
        print(f"  ⏭️  {releases_csv.relative_to(ROOT_DIR)} already has data for {date}")
    
    # Update packages CSV if we have package data
    if packages_config:
        packages_csv = repo_data_dir / "packages.csv"
        
        # Get existing dates from packages.csv
        existing_dates = set()
        if packages_csv.exists():
            with open(packages_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_dates.add(row.get("date", ""))
        
        # Check if we need to backfill historical data (less than 30 days of data)
        if len(existing_dates) < 30:
            print(f"  📊 Backfilling npm historical data...")
            historical_data = collect_package_history(packages_config, existing_dates)
            if historical_data:
                ensure_csv_headers(packages_csv, PACKAGES_HEADERS)
                # Sort by date and append
                historical_data.sort(key=lambda x: x["date"])
                with open(packages_csv, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    for pkg in historical_data:
                        writer.writerow([
                            pkg["date"],
                            pkg["source"],
                            pkg["package"],
                            pkg["daily_downloads"],
                            pkg["weekly_downloads"],
                        ])
                print(f"  ✅ Added {len(historical_data)} days of historical npm data")
        
        # Add today's data if not already present
        if packages_data:
            if update_packages_csv(packages_csv, date, packages_data):
                print(f"  ✅ Updated {packages_csv.relative_to(ROOT_DIR)}")
            else:
                print(f"  ⏭️  {packages_csv.relative_to(ROOT_DIR)} already has data for {date}")
    
    # Update star history CSV (only if we got data and file doesn't exist or is small)
    star_history_csv = repo_data_dir / "star_history.csv"
    if star_history and (not star_history_csv.exists() or star_history_csv.stat().st_size < 500):
        ensure_csv_headers(star_history_csv, STAR_HISTORY_HEADERS)
        with open(star_history_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(STAR_HISTORY_HEADERS)
            for record in star_history:
                writer.writerow([record["date"], record["stars"]])
        print(f"  ✅ Updated {star_history_csv.relative_to(ROOT_DIR)}")
    
    return True


def main() -> int:
    """Main entry point."""
    print("🚀 GitHub Analytics Collector")
    print(f"   Token: {'✅ Configured' if GITHUB_TOKEN else '❌ Not set (limited access)'}")
    
    # Load configuration
    if not CONFIG_FILE.exists():
        print(f"❌ Config file not found: {CONFIG_FILE}")
        return 1
    
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)
    
    repos_config = config.get("repos", [])
    if not repos_config:
        print("❌ No repositories configured")
        return 1
    
    print(f"   Repos: {len(repos_config)}")
    
    # Get today's date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"   Date:  {today}")
    
    # Collect metrics for each repo
    success_count = 0
    for repo_entry in repos_config:
        try:
            # Support both old format (string) and new format (dict)
            if isinstance(repo_entry, str):
                repo_name = repo_entry
                packages_config = None
            else:
                repo_name = repo_entry.get("repo")
                packages_config = repo_entry.get("packages", {})
            
            if not repo_name:
                print(f"  ⚠️  Skipping invalid config entry: {repo_entry}")
                continue
                
            if collect_repo(repo_name, today, packages_config):
                success_count += 1
        except Exception as e:
            print(f"  ❌ Error collecting {repo_entry}: {e}")
    
    print(f"\n✨ Done! Collected {success_count}/{len(repos_config)} repositories")
    return 0 if success_count == len(repos_config) else 1


if __name__ == "__main__":
    sys.exit(main())
