# Earthquake Tracker  
A simple Python tool to scrape and track earthquake data for the Philippines from the PHIVOLCS site, store it, and provide a CSV output.

## 🧭 Overview  
This project provides the following functionality:  
- Connects to the PHIVOLCS website / RSS or data feed and scrapes recent earthquake events.  
- Stores the events (in this repo there’s a sample `earthquakes.csv` file).  
- Provides a `scrape_phivolcs.py` script to automate retrieval of new events.  
- Has a `requirements.txt` listing dependencies needed.

## 📂 Repository Structure  
- `scrape_phivolcs.py` – main Python script to fetch new earthquake data.  
- `earthquakes.csv` – sample CSV file containing previously scraped events.  
- `requirements.txt` – list of Python dependencies for the project.  
- `.github/workflows/` – GitHub Actions workflow(s) for automation (if applicable).  

## 🚀 Getting Started  
### Prerequisites  
- Python 3.7+ (or whichever version the project supports).  
- Internet access to fetch PHIVOLCS data.  
- (Optional) A scheduler (cron job, GitHub Actions) if you’d like to run the scraper periodically.

### Installation  
1. Clone the repo:  
   ```bash
   git clone https://github.com/chamthesleeptalker/earthquake-tracker.git  
   cd earthquake-tracker  
   ```
2. Install dependencies:  
   ```bash
   pip install -r requirements.txt  
   ```
3. Run the scraper:  
   ```bash
   python scrape_phivolcs.py  
   ```
   This will fetch recent quake events and update or produce a CSV file.

## 🧮 Usage  
- After execution, open `earthquakes.csv` to view earthquake events with their attributes (e.g., date/time, magnitude, location, depth).  
- You can incorporate this CSV into further analysis, dashboards, or alerts.  
- If you wish, schedule the script to run at regular intervals (e.g., hourly/daily) so you have continuous updates.

## ✨ Features & Benefits  
- Focused on Philippine seismic events via PHIVOLCS — useful for local monitoring.  
- CSV output makes it easy to integrate with other tools (Excel, Python/pandas, dashboards).  
- Lightweight: Easy to run, minimal dependencies.  
- Extensible: You could adapt the scraper to other sources (USGS, etc) or expand into dashboards.

## 🔧 Customization & Extensions  
Here are some ways you can build on this project:  
- **Add more data sources**: Extend to scrape from the USGS earthquake API, or other global feeds.  
- **Alerting**: Automatically send email, SMS or push notification when an event above a threshold magnitude occurs.  
- **Visualization**: Create a dashboard (e.g., using Plotly, Streamlit) to plot events on a map, analyze depth vs magnitude, etc.  
- **Storage**: Instead of CSV, store events in a database (PostgreSQL, SQLite) for long‑term historical analysis.  
- **Automation**: Use GitHub Actions (already partially set up) or cron to run the scraper automatically on a schedule.

## 📋 Sample Data Format  
Here’s an example of what a row in `earthquakes.csv` might look like:  
```
date_time, location, magnitude, depth, …  
2025‑10‑12 04:35:12, San Fernando, La Union, Philippines, 4.3, 10 km  
```
Each column will correspond to the parsed attributes from PHIVOLCS feed.
