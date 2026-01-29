/**
 * GitHub Analytics Dashboard - Application JavaScript
 * Loads CSV data and renders Chart.js visualizations
 */

const DATA_BASE_URL = '/data';

// Chart.js default configuration
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
Chart.defaults.color = '#6b7280';

// Color palette
const COLORS = {
    primary: 'rgb(59, 130, 246)',
    primaryBg: 'rgba(59, 130, 246, 0.1)',
    secondary: 'rgb(16, 185, 129)',
    secondaryBg: 'rgba(16, 185, 129, 0.1)',
    accent: 'rgb(249, 115, 22)',
    accentBg: 'rgba(249, 115, 22, 0.1)',
    purple: 'rgb(139, 92, 246)',
    purpleBg: 'rgba(139, 92, 246, 0.1)',
    pink: 'rgb(236, 72, 153)',
    pinkBg: 'rgba(236, 72, 153, 0.1)',
    yellow: 'rgb(234, 179, 8)',
    yellowBg: 'rgba(234, 179, 8, 0.1)',
    cyan: 'rgb(6, 182, 212)',
    cyanBg: 'rgba(6, 182, 212, 0.1)',
};

const ASSET_COLORS = [
    COLORS.primary, COLORS.secondary, COLORS.accent,
    COLORS.purple, COLORS.pink, COLORS.yellow, COLORS.cyan,
    'rgb(244, 63, 94)', 'rgb(168, 85, 247)', 'rgb(34, 197, 94)',
];

// Utility Functions
function getRepoFromURL() {
    const params = new URLSearchParams(window.location.search);
    return params.get('repo');
}

function parseCSV(text) {
    const lines = text.trim().split('\n');
    if (lines.length < 2) return [];
    
    const headers = lines[0].split(',');
    const data = [];
    
    for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',');
        const row = {};
        headers.forEach((h, idx) => {
            row[h.trim()] = values[idx]?.trim() || '';
        });
        data.push(row);
    }
    
    return data;
}

function formatNumber(num) {
    if (!num || num === '') return '—';
    const n = parseInt(num);
    if (isNaN(n)) return '—';
    if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
    if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
    return n.toLocaleString();
}

function formatBytes(bytes) {
    if (!bytes) return '—';
    const b = parseInt(bytes);
    if (b >= 1073741824) return (b / 1073741824).toFixed(1) + ' GB';
    if (b >= 1048576) return (b / 1048576).toFixed(1) + ' MB';
    if (b >= 1024) return (b / 1024).toFixed(1) + ' KB';
    return b + ' B';
}

function formatDate(dateStr) {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

// Data Loading
async function loadAggregateData(repo) {
    const [owner, name] = repo.split('/');
    const url = `${DATA_BASE_URL}/${owner}/${name}/aggregate.csv`;
    
    try {
        const response = await fetch(url);
        if (!response.ok) throw new Error('Not found');
        const text = await response.text();
        return parseCSV(text);
    } catch (error) {
        console.error('Failed to load aggregate data:', error);
        return [];
    }
}

async function loadReleasesData(repo) {
    const [owner, name] = repo.split('/');
    const url = `${DATA_BASE_URL}/${owner}/${name}/releases.csv`;
    
    try {
        const response = await fetch(url);
        if (!response.ok) throw new Error('Not found');
        const text = await response.text();
        return parseCSV(text);
    } catch (error) {
        console.error('Failed to load releases data:', error);
        return [];
    }
}

async function loadStarHistory(repo) {
    const [owner, name] = repo.split('/');
    const url = `${DATA_BASE_URL}/${owner}/${name}/star_history.csv`;
    
    try {
        const response = await fetch(url);
        if (!response.ok) throw new Error('Not found');
        const text = await response.text();
        return parseCSV(text);
    } catch (error) {
        console.error('Failed to load star history:', error);
        return [];
    }
}

async function loadPackagesData(repo) {
    const [owner, name] = repo.split('/');
    const url = `${DATA_BASE_URL}/${owner}/${name}/packages.csv`;
    
    try {
        const response = await fetch(url);
        if (!response.ok) throw new Error('Not found');
        const text = await response.text();
        return parseCSV(text);
    } catch (error) {
        console.log('No packages data available:', error.message);
        return [];
    }
}

// Check if traffic data exists
function hasTrafficData(data) {
    return data.some(d => d.views && d.views !== '' && parseInt(d.views) > 0);
}

// Stats Display
function updateStats(data, hasTraffic) {
    if (data.length === 0) return;
    
    const latest = data[data.length - 1];
    
    document.getElementById('stat-stars').textContent = formatNumber(latest.stars);
    document.getElementById('stat-forks').textContent = formatNumber(latest.forks);
    document.getElementById('stat-watchers').textContent = formatNumber(latest.watchers);
    document.getElementById('stat-downloads').textContent = formatNumber(latest.releases_downloads);
    document.getElementById('stat-issues').textContent = formatNumber(latest.open_issues);
    document.getElementById('stat-prs').textContent = formatNumber(latest.open_prs);
    
    // Hide or show traffic stat
    const viewsStat = document.getElementById('stat-views-card');
    if (viewsStat) {
        if (hasTraffic) {
            document.getElementById('stat-views').textContent = formatNumber(latest.views);
        } else {
            viewsStat.style.display = 'none';
        }
    }
}

// Chart Rendering
function createTrafficChart(ctx, data) {
    const labels = data.map(d => formatDate(d.date));
    const views = data.map(d => parseInt(d.views) || null);
    const viewsUnique = data.map(d => parseInt(d.views_unique) || null);
    const clones = data.map(d => parseInt(d.clones) || null);
    const clonesUnique = data.map(d => parseInt(d.clones_unique) || null);
    
    return new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [
                {
                    label: 'Views',
                    data: views,
                    borderColor: COLORS.primary,
                    backgroundColor: COLORS.primaryBg,
                    tension: 0.3,
                    fill: true,
                },
                {
                    label: 'Unique Views',
                    data: viewsUnique,
                    borderColor: COLORS.secondary,
                    backgroundColor: 'transparent',
                    tension: 0.3,
                    borderDash: [5, 5],
                },
                {
                    label: 'Clones',
                    data: clones,
                    borderColor: COLORS.accent,
                    backgroundColor: COLORS.accentBg,
                    tension: 0.3,
                },
                {
                    label: 'Unique Clones',
                    data: clonesUnique,
                    borderColor: COLORS.purple,
                    backgroundColor: 'transparent',
                    tension: 0.3,
                    borderDash: [5, 5],
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { intersect: false, mode: 'index' },
            plugins: { legend: { position: 'bottom' } },
            scales: { y: { beginAtZero: true } },
        },
    });
}

function createGrowthChart(ctx, data, starHistory) {
    // If we have star history, use it for historical context with time scale
    let chartData;
    
    if (starHistory && starHistory.length > 1) {
        chartData = starHistory.map(d => ({
            x: d.date,
            y: parseInt(d.stars) || 0
        }));
    } else {
        chartData = data.map(d => ({
            x: d.date,
            y: parseInt(d.stars) || 0
        }));
    }
    
    return new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'Stars',
                    data: chartData,
                    borderColor: COLORS.primary,
                    backgroundColor: COLORS.primaryBg,
                    tension: 0.3,
                    fill: true,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { intersect: false, mode: 'index' },
            plugins: { legend: { position: 'bottom' } },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'day',
                        displayFormats: { day: 'MMM d' }
                    },
                    title: { display: false }
                },
                y: { beginAtZero: false }
            },
        },
    });
}

function createDownloadsChart(ctx, data) {
    const labels = data.map(d => formatDate(d.date));
    const downloads = data.map(d => parseInt(d.releases_downloads) || 0);
    
    return new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [
                {
                    label: 'Total Downloads',
                    data: downloads,
                    borderColor: COLORS.accent,
                    backgroundColor: COLORS.accentBg,
                    tension: 0.3,
                    fill: true,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom' } },
            scales: { y: { beginAtZero: true } },
        },
    });
}

function createIssuesChart(ctx, data) {
    const labels = data.map(d => formatDate(d.date));
    const issues = data.map(d => parseInt(d.open_issues) || 0);
    const prs = data.map(d => parseInt(d.open_prs) || 0);
    
    return new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [
                {
                    label: 'Open Issues',
                    data: issues,
                    borderColor: COLORS.pink,
                    backgroundColor: COLORS.pinkBg,
                    tension: 0.3,
                    fill: true,
                },
                {
                    label: 'Open PRs',
                    data: prs,
                    borderColor: COLORS.purple,
                    backgroundColor: COLORS.purpleBg,
                    tension: 0.3,
                    fill: true,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom' } },
            scales: { y: { beginAtZero: true } },
        },
    });
}

function createDailyDownloadsChart(ctx, releasesData) {
    if (releasesData.length === 0) return null;
    
    // Deduplicate data
    const dedupedData = deduplicateReleases(releasesData);
    
    // Group by date and sum deltas
    const dailyDownloads = new Map();
    dedupedData.forEach(row => {
        const date = row.date;
        const delta = parseInt(row.downloads_delta) || 0;
        dailyDownloads.set(date, (dailyDownloads.get(date) || 0) + delta);
    });
    
    const sortedDates = Array.from(dailyDownloads.keys()).sort();
    const labels = sortedDates.map(d => formatDate(d));
    const data = sortedDates.map(d => dailyDownloads.get(d));
    
    return new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'Downloads per Day',
                    data: data,
                    backgroundColor: COLORS.secondary,
                    borderColor: COLORS.secondary,
                    borderWidth: 1,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom' } },
            scales: { y: { beginAtZero: true } },
        },
    });
}

function createReleasesChart(ctx, releasesData) {
    if (releasesData.length === 0) {
        ctx.canvas.parentElement.innerHTML += '<p class="no-data">No release data available</p>';
        return null;
    }
    
    // Deduplicate and get latest date's data
    const dedupedData = deduplicateReleases(releasesData);
    const latestDate = dedupedData[dedupedData.length - 1].date;
    const latestData = dedupedData.filter(d => d.date === latestDate);
    
    // Group by tag
    const releaseMap = new Map();
    latestData.forEach(row => {
        const tag = row.tag;
        if (!releaseMap.has(tag)) {
            releaseMap.set(tag, []);
        }
        releaseMap.get(tag).push({
            name: row.asset_name,
            downloads: parseInt(row.download_count) || 0,
        });
    });
    
    // Sort releases by version (newest first) using semver-like comparison
    const parseVersion = (tag) => {
        const match = tag.match(/(\d+)\.(\d+)\.(\d+)/);
        if (match) {
            return [parseInt(match[1]), parseInt(match[2]), parseInt(match[3])];
        }
        return [0, 0, 0];
    };
    
    const releases = Array.from(releaseMap.entries())
        .map(([tag, assets]) => ({
            tag,
            assets,
            total: assets.reduce((sum, a) => sum + a.downloads, 0),
        }))
        .sort((a, b) => {
            const vA = parseVersion(a.tag);
            const vB = parseVersion(b.tag);
            for (let i = 0; i < 3; i++) {
                if (vB[i] !== vA[i]) return vB[i] - vA[i];
            }
            return b.tag.localeCompare(a.tag);
        })
        .slice(0, 10); // Top 10 releases
    
    // Build chart data - just totals per release
    const labels = releases.map(r => r.tag);
    const totals = releases.map(r => r.total);
    
    return new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Total Downloads',
                data: totals,
                backgroundColor: COLORS.primary,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                y: { beginAtZero: true },
            },
        },
    });
}

// Deduplicate releases data - keep only one entry per date/tag/asset
function deduplicateReleases(releasesData) {
    const seen = new Set();
    return releasesData.filter(row => {
        const key = `${row.date}|${row.tag}|${row.asset_name}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });
}

// Release Cards
function createReleaseCards(releasesData, container) {
    if (releasesData.length === 0) {
        container.innerHTML = '<p class="no-data">No release data available</p>';
        return;
    }
    
    // Deduplicate and get latest date's data
    const dedupedData = deduplicateReleases(releasesData);
    const latestDate = dedupedData[dedupedData.length - 1].date;
    const latestData = dedupedData.filter(d => d.date === latestDate);
    
    // Group by tag
    const releaseMap = new Map();
    latestData.forEach(row => {
        const tag = row.tag;
        if (!releaseMap.has(tag)) {
            releaseMap.set(tag, []);
        }
        releaseMap.get(tag).push({
            name: row.asset_name,
            downloads: parseInt(row.download_count) || 0,
            size: parseInt(row.asset_size) || 0,
            delta: parseInt(row.downloads_delta) || 0,
        });
    });
    
    // Sort releases by version (newest first) using semver-like comparison
    const releases = Array.from(releaseMap.entries())
        .map(([tag, assets]) => ({
            tag,
            assets: assets.sort((a, b) => a.name.localeCompare(b.name)), // Sort by name for consistency
            total: assets.reduce((sum, a) => sum + a.downloads, 0),
        }))
        .sort((a, b) => {
            // Extract version numbers for comparison (handles v1.2.3 format)
            const parseVersion = (tag) => {
                const match = tag.match(/(\d+)\.(\d+)\.(\d+)/);
                if (match) {
                    return [parseInt(match[1]), parseInt(match[2]), parseInt(match[3])];
                }
                return [0, 0, 0];
            };
            const vA = parseVersion(a.tag);
            const vB = parseVersion(b.tag);
            // Compare major, minor, patch (descending = newest first)
            for (let i = 0; i < 3; i++) {
                if (vB[i] !== vA[i]) return vB[i] - vA[i];
            }
            return b.tag.localeCompare(a.tag); // Fallback to string comparison
        });
    
    container.innerHTML = releases.map((release, idx) => `
        <div class="release-card">
            <div class="release-header">
                <h4>${release.tag}</h4>
                <span class="release-total">${formatNumber(release.total)} downloads</span>
            </div>
            <div class="release-assets">
                ${release.assets.map((asset, aidx) => `
                    <div class="asset-row">
                        <span class="asset-color" style="background: ${ASSET_COLORS[aidx % ASSET_COLORS.length]}"></span>
                        <span class="asset-name">${asset.name}</span>
                        <span class="asset-size">${formatBytes(asset.size)}</span>
                        <span class="asset-downloads">${formatNumber(asset.downloads)}</span>
                        ${asset.delta > 0 ? `<span class="asset-delta">+${asset.delta}</span>` : ''}
                    </div>
                `).join('')}
            </div>
        </div>
    `).join('');
}

// Create npm packages chart
function createPackagesChart(ctx, data) {
    if (data.length === 0) return null;
    
    // Group by package
    const packages = {};
    data.forEach(d => {
        const pkg = d.package;
        if (!packages[pkg]) packages[pkg] = [];
        packages[pkg].push({
            date: d.date,
            dailyDownloads: parseInt(d.daily_downloads) || 0,
            weeklyDownloads: parseInt(d.weekly_downloads) || 0,
        });
    });
    
    // Create datasets for each package (using weekly downloads for better visibility)
    const datasets = [];
    const colors = [COLORS.accent, COLORS.purple, COLORS.pink, COLORS.cyan, COLORS.yellow];
    let colorIdx = 0;
    
    for (const [pkg, downloads] of Object.entries(packages)) {
        // Sort by date
        downloads.sort((a, b) => a.date.localeCompare(b.date));
        
        const color = colors[colorIdx % colors.length];
        const bgColor = color.replace('rgb', 'rgba').replace(')', ', 0.1)');
        
        datasets.push({
            label: pkg,
            data: downloads.map(d => ({
                x: new Date(d.date),
                y: d.weeklyDownloads,
            })),
            borderColor: color,
            backgroundColor: bgColor,
            tension: 0.3,
            fill: true,
        });
        
        colorIdx++;
    }
    
    return new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                intersect: false,
                mode: 'index',
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'day',
                        displayFormats: {
                            day: 'MMM d',
                        },
                    },
                    grid: { display: false },
                },
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: (v) => formatNumber(v),
                    },
                },
            },
            plugins: {
                legend: {
                    position: 'top',
                },
                tooltip: {
                    callbacks: {
                        label: (ctx) => `${ctx.dataset.label}: ${formatNumber(ctx.parsed.y)} downloads`,
                    },
                },
            },
        },
    });
}

// Get latest npm weekly downloads from snapshot or calculate
function getLatestNpmStats(packagesData) {
    if (packagesData.length === 0) return null;
    
    // Get the most recent data point
    const sortedData = [...packagesData].sort((a, b) => 
        b.date.localeCompare(a.date)
    );
    
    // Sum up all packages for the latest date
    const latestDate = sortedData[0].date;
    const latestDayData = sortedData.filter(d => d.date === latestDate);
    
    const totalDaily = latestDayData.reduce((sum, d) => sum + (parseInt(d.daily_downloads) || 0), 0);
    const totalWeekly = latestDayData.reduce((sum, d) => sum + (parseInt(d.weekly_downloads) || 0), 0);
    
    return {
        date: latestDate,
        daily: totalDaily,
        weekly: totalWeekly,
        packages: latestDayData.map(d => d.package),
    };
}

// Main Initialization
async function init() {
    const repo = getRepoFromURL();
    
    if (!repo) {
        document.getElementById('repo-title').textContent = 'No repository specified';
        document.getElementById('repo-subtitle').textContent = 'Add ?repo=owner/name to the URL';
        return;
    }
    
    // Update header
    document.getElementById('repo-title').textContent = repo;
    document.getElementById('repo-subtitle').innerHTML = 
        `<a href="https://github.com/${repo}" target="_blank">View on GitHub ↗</a>`;
    document.title = `${repo} - GitHub Analytics`;
    
    // Load data
    const [aggregateData, releasesData, starHistory, packagesData] = await Promise.all([
        loadAggregateData(repo),
        loadReleasesData(repo),
        loadStarHistory(repo),
        loadPackagesData(repo),
    ]);
    
    if (aggregateData.length === 0) {
        document.querySelector('.stats-cards').innerHTML = 
            '<p class="no-data">No data collected yet. Run the collector workflow first.</p>';
        return;
    }
    
    // Check for traffic data
    const hasTraffic = hasTrafficData(aggregateData);
    
    // Update stats
    updateStats(aggregateData, hasTraffic);
    
    // Update npm stat if packages data available
    const npmStats = getLatestNpmStats(packagesData);
    const npmStatEl = document.getElementById('stat-npm');
    const npmStatCard = document.getElementById('stat-npm-card');
    if (npmStats && npmStatEl) {
        // Show weekly downloads (more meaningful than daily which can be 0)
        npmStatEl.textContent = formatNumber(npmStats.weekly);
    } else if (npmStatCard) {
        npmStatCard.style.display = 'none';
    }
    
    // Hide traffic section if no data
    const trafficSection = document.getElementById('traffic-section');
    if (trafficSection && !hasTraffic) {
        trafficSection.style.display = 'none';
    } else if (trafficSection) {
        createTrafficChart(
            document.getElementById('traffic-chart').getContext('2d'),
            aggregateData
        );
    }
    
    // Create growth chart (with star history if available)
    createGrowthChart(
        document.getElementById('growth-chart').getContext('2d'),
        aggregateData,
        starHistory
    );
    
    // Create downloads charts
    createDownloadsChart(
        document.getElementById('downloads-chart').getContext('2d'),
        aggregateData
    );
    
    // Create daily downloads chart
    const dailyDownloadsCtx = document.getElementById('daily-downloads-chart');
    if (dailyDownloadsCtx) {
        createDailyDownloadsChart(
            dailyDownloadsCtx.getContext('2d'),
            releasesData
        );
    }
    
    // Create issues chart
    const issuesCtx = document.getElementById('issues-chart');
    if (issuesCtx) {
        createIssuesChart(
            issuesCtx.getContext('2d'),
            aggregateData
        );
    }
    
    // Create npm packages chart if data available
    const npmSection = document.getElementById('npm-section');
    const npmChartCtx = document.getElementById('npm-chart');
    if (packagesData.length > 0 && npmChartCtx) {
        createPackagesChart(
            npmChartCtx.getContext('2d'),
            packagesData
        );
    } else if (npmSection) {
        npmSection.style.display = 'none';
    }
    
    // Create releases chart
    createReleasesChart(
        document.getElementById('releases-chart').getContext('2d'),
        releasesData
    );
    
    // Create release cards
    const releaseCardsContainer = document.getElementById('release-cards');
    if (releaseCardsContainer) {
        createReleaseCards(releasesData, releaseCardsContainer);
    }
}

// Start the app
init();
