# 📊 GitHub Analytics

A simple dashboard to track GitHub repository metrics over time. Automatically collects stars, forks, issues, release downloads, and npm package stats daily via GitHub Actions.

[View dashboard](https://metulev.com/project-analytics)

## Features

- **Historical tracking** - Daily snapshots of repo metrics stored as CSV
- **Star history** - Reconstructs full star history like star-history.com
- **Release downloads** - Per-asset download counts with daily deltas
- **npm package tracking** - Historical download stats (backfills up to 1 year)
- **Zero database** - All data stored in CSV files, served via GitHub Pages

## Add a repo

1. Edit `config.json` to add your repos:
   ```json
   {
     "repos": [
       "owner/repo",
       {
         "repo": "owner/repo",
         "packages": {
           "npm": ["@scope/package-name"]
         }
       }
     ]
   }
   ```
2. Submit a PR.

Or just clone the project and host it yourself for your own repos.

## Local Development

```bash
python -m venv .venv
.venv/Scripts/activate  # or source .venv/bin/activate
pip install requests
python scripts/collect.py
python -m http.server 8080
```


*This project was vibe coded with [GitHub Copilot](https://github.com/features/copilot). 🤙*
