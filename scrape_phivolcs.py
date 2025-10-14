import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime, timedelta
import os
import pandas as pd
from datetime import timedelta

URL = "https://earthquake.phivolcs.dost.gov.ph/"
EARTHQUAKE_CSV_FILE = "earthquakes.csv"
EARTHQUAKE_PAST_HOUR_CSV_FILE = "earthquakes_past_hour.csv"
EARTHQUAKE_TODAY_CSV_FILE = "earthquakes_today.csv"
EARTHQUAKE_PAST_7_DAYS_CSV_FILE = "earthquakes_past_7_days.csv"
EARTHQUAKE_PAST_24_HOURS_CSV_FILE = "earthquakes_past_24_hours.csv"
EARTHQUAKE_SUMMARY_CSV_FILE = "earthquake_summary_stats.csv"
EARTHQUAKE_TEMPORAL_CSV_FILE = "earthquake_temporal_stats.csv"
TOP_TEN_EVENTS_CSV_FILE = "top_ten_events.csv"
TOP_TEN_LOCATIONS_CSV_FILE = "top_ten_locations.csv"


COLUMN_MAP = {
    "Date - Time (Philippine Time)": "datetime_ph",
    "Latitude (ºN)": "latitude",
    "Longitude (ºE)": "longitude",
    "Depth (km)": "depth_km",
    "Magnitude": "magnitude",
    "Location": "location" 
}

def fetch_html():
    resp = requests.get(
        URL,
        verify=False, 
        timeout=10)
    resp.raise_for_status()
    return resp.text

def parse_location(location_str):
    
    full_string = location_str.split(" ")
    
    if location_str:
        distance = full_string[0].split("km")[0]
        bearing = f"{full_string[1]} {full_string[2]} {full_string[3]}"
        ref_location = " ".join(full_string[5:])
        return distance, bearing.encode("latin1").decode("utf-8"), ref_location
    else:
        return None, None, location_str.encode("latin1").decode("utf-8")
    
def parse_table(html):
    
    soup = BeautifulSoup(html,"html.parser")
    tables = soup.find_all("table")
    if len(tables) < 3:
        raise Exception("Expected at least 3 tables, found less.")
    table = tables[2]
    tbody = table.find("tbody")
    rows = tbody.find_all("tr")

    header_cells = rows[0].find_all("th")
    raw_headers = [th.get_text(separator=" ", strip=True) for th in header_cells]
    headers = [COLUMN_MAP.get(h, h.lower().replace(" ","_")) for h in raw_headers]
    headers = [h.encode("latin1").strip().decode('utf-8', errors='ignore') for h in headers]
    final_headers = ["datetime_iso"] + headers + ["distance_km", "bearing", "reference_location"]
    records = []
    
    #print(rows[1:]) #check
    for row in rows[1:]:
        cells = row.find_all("td")
        #print("after cells") #check
        #print(f"{len(cells)} , {len(headers)}") #check
        if len(cells) != len(headers):
            continue
        
        raw_values = []
        

        for i,td in enumerate(cells):
            # print(f'td check: {td.find("a")}')
            if i==0 and td.find("a"):
                raw_values.append(td.find("a").get_text(strip=True))
                # print(raw_values)
            else:
                raw_values.append(td.get_text(strip=True))
                # print(raw_values)

        if len(raw_values) != len(headers):
            continue

        record = dict(zip(headers, raw_values))
        dt_ph_str = record["datetime_ph"]
       

        try:
            dt_ph = datetime.strptime(dt_ph_str, "%d %B %Y - %I:%M %p")
            dt_utc = dt_ph
            record["datetime_iso"] = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        except Exception:
            print("Skipping row with invalid datetime: {dt_ph_str}")
            continue
       
        loc_str = record.get("location", "")
        
        dist, bearing, ref = parse_location(loc_str)
        
        record["distance_km"] = dist
        record["bearing"] = bearing
        record["reference_location"] = ref
        record["location"] = record["location"].encode("latin1").strip().decode('utf-8', errors='ignore')
        full_record = {key: record.get(key, "") for key in final_headers}
        records.append(full_record)
      
    return final_headers, records

def load_existing_timestamps():
    print(f'load_existing_timestamps_def')
    if not os.path.exists(EARTHQUAKE_CSV_FILE):
        return set()
    with open(EARTHQUAKE_CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["datetime_iso"] for row in reader}
    
def generate_summary_stats(past_hour, today, past_24h, past_7d):
    """ Generates summary statistics from segmented earthquake data.
    Each parameter is a DataFrame corresponding to a time segment."""

    print(f'generate_summary_stats_def')
    
    # Overview
    # Total (1h)
    total_1h = len(past_hour)
    # print(f'total_1h: {total_1h}')

    # Total (today)
    total_today  = len(today)
    # print(f'total_today: {total_today}')    

    # Total (24h)
    total_24h = len(past_24h)
    # print(f'total_24h: {total_24h}')

    # Total (7d)            
    total_7d = len(past_7d) 
    # print(f'total_7d: {total_7d}')

    # Key insights (7 days)
    # Strongest event
    strongest_event = past_7d.loc[past_7d['mag'].idxmax()] if not past_7d.empty else None
    # print(f'strongest_event: {strongest_event}')

    # Most active region
    most_active_region = past_7d['reference_location'].mode()[0] if not past_7d.empty else None
    # print(f'most_active_region: {most_active_region}')

    # Convert depth_km to numeric for calculations
    past_7d['depth_km'] = pd.to_numeric(past_7d['depth_km'], errors='coerce')
    average_depth = past_7d['depth_km'].mean() if not past_7d.empty else None
    if average_depth is not None:
        average_depth = round(average_depth, 2)
    # print(f'average_depth: {average_depth}')

    # Average magnitude
    past_7d['mag'] = pd.to_numeric(past_7d['mag'], errors='coerce')
    average_magnitude = past_7d['mag'].mean() if not past_7d.empty else None
    if average_magnitude is not None:
        average_magnitude = round(average_magnitude, 2)
    # print(f'average_magnitude: {average_magnitude}')

    # Top 10 Seismic events (7 days)
    top_10_events = past_7d.nlargest(10, 'mag') if not past_7d.empty else pd.DataFrame()
    # print(f'top_10_events: {top_10_events}')

    # Top 10 Locations (7 days)
    top_10_locations = past_7d['reference_location'].value_counts().head(10) if not past_7d.empty else pd.Series()
    # Create a new DataFrame from top_10_locations
    top_10_locations_df = top_10_locations.reset_index()
    top_10_locations_df.columns = ['reference_location', 'count']
    # print('Top 10 Locations DataFrame:')
    # print(top_10_locations_df)

    # Prepare summary stats for CSV
    summary_headers = [
        "strongest_event",
        "most_active_region",
        "average_depth",
        "average_magnitude"
    ]

    # Extract values for the summary row
    summary_values = [
        f'<p id="strongest_event">Magnitude: { strongest_event["mag"] if strongest_event is not None else None }<br>Depth: {strongest_event["depth_km"] if strongest_event is not None else None} km<br> {strongest_event["location"] if strongest_event is not None else None}</p>',
        most_active_region,
        average_depth,
        average_magnitude
    ]

    summary_titles = [
        "Strongest Event",
        "Most Active Region",
        "Average Depth (km)",
        "Average Magnitude"
    ]

    # Write summary stats to CSV with 'figures' and 'values' columns
    if os.path.exists(EARTHQUAKE_SUMMARY_CSV_FILE):
        print(f"'{EARTHQUAKE_SUMMARY_CSV_FILE}' exists — replacing old file.")
        os.remove(EARTHQUAKE_SUMMARY_CSV_FILE)

    with open(EARTHQUAKE_SUMMARY_CSV_FILE, "w", newline="", encoding="utf-8") as f:
        print(f"Creating new file '{EARTHQUAKE_SUMMARY_CSV_FILE}'.")
        writer = csv.writer(f)
        writer.writerow(["figures", "values", "titles"])
        for header, value, titles in zip(summary_headers, summary_values, summary_titles):
            writer.writerow([header, value, titles])
    print(f"Saved summary stats to {EARTHQUAKE_SUMMARY_CSV_FILE}")

     # Prepare summary stats for CSV
    temporal_headers = [
        "total_1h",
        "total_today",
        "total_24h",
        "total_7d",
    ]

    # Extract values for the summary row
    temporal_values = [
        total_1h,
        total_today,
        total_24h,
        total_7d,
    ]

    temporal_titles = [
        "in the Past Hour",
        "today",
        "in the Past 24 Hours",
        "in the Past 7 Days",
    ]

    # Write summary stats to CSV with 'figures' and 'values' columns
    
    if os.path.exists(EARTHQUAKE_TEMPORAL_CSV_FILE):
        print(f"'{EARTHQUAKE_TEMPORAL_CSV_FILE}' exists — replacing old file.")
        os.remove(EARTHQUAKE_TEMPORAL_CSV_FILE)

    with open(EARTHQUAKE_TEMPORAL_CSV_FILE, "w", newline="", encoding="utf-8") as f:
        print(f"Creating new file '{EARTHQUAKE_TEMPORAL_CSV_FILE}'.")
        writer = csv.writer(f)
        writer.writerow(["figures", "values", "titles"])
        for header, value, titles in zip(temporal_headers, temporal_values, temporal_titles):
            writer.writerow([header, value, titles])
    print(f"Saved summary stats to {EARTHQUAKE_TEMPORAL_CSV_FILE}")


    # Save to CSV Files
    output_files = {
        TOP_TEN_EVENTS_CSV_FILE: top_10_events,
        TOP_TEN_LOCATIONS_CSV_FILE: top_10_locations_df,
    }

    # Save to CSV Files (overwrite if existing)
    for filename, subset in output_files.items():
        if os.path.exists(filename):
            print(f"'{filename}' exists — replacing old file.")
            os.remove(filename)

        subset.to_csv(filename, index=False, mode="w")
        print(f"Saved {len(subset)} entries to {filename}")

def segment_earthquake_data(data):
    """
    Segments earthquake data (list of dicts) into four time-based categories
    and writes each category to a separate CSV file.
    """

    # Convert array of dictionaries to DataFrame
    df = pd.DataFrame(data)

    # Ensure datetime column exists
    if "datetime_iso" not in df.columns:
        raise ValueError("Input data must include a 'datetime_iso' field.")

    # Convert to datetime
    df["datetime_iso"] = pd.to_datetime(df["datetime_iso"], utc=True)

    # Use the most recent timestamp as the reference point
    latest_time = df["datetime_iso"].max()
    current_date = latest_time.date()

    print(f"Reference timestamp: {latest_time} (UTC)")

    # Segment Data 
    past_hour = df[df["datetime_iso"] >= latest_time - timedelta(hours=1)]
    today = df[df["datetime_iso"].dt.date == current_date]
    past_24h = df[df["datetime_iso"] >= latest_time - timedelta(hours=24)]
    past_7d = df[df["datetime_iso"] >= latest_time - timedelta(days=7)]

    # Format datetime_iso
    def format_datetime_column(df_segment):
        df_segment = df_segment.copy()
        df_segment["datetime_iso"] = df_segment["datetime_iso"].dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        return df_segment

    past_hour = format_datetime_column(past_hour)
    today = format_datetime_column(today)
    past_24h = format_datetime_column(past_24h)
    past_7d = format_datetime_column(past_7d)

    generate_summary_stats(past_hour, today, past_24h, past_7d)

    # Save to CSV Files
    output_files = {
        EARTHQUAKE_PAST_HOUR_CSV_FILE: past_hour,
        EARTHQUAKE_TODAY_CSV_FILE: today,
        EARTHQUAKE_PAST_24_HOURS_CSV_FILE: past_24h,
        EARTHQUAKE_PAST_7_DAYS_CSV_FILE: past_7d,
    }

    # Save to CSV Files (overwrite if existing)
    for filename, subset in output_files.items():
        if os.path.exists(filename):
            print(f"'{filename}' exists — replacing old file.")
            os.remove(filename)

        subset.to_csv(filename, index=False, mode="w")
        print(f"Saved {len(subset)} entries to {filename}")

    

    print("\nSegmentation complete.")


def save_new_records(headers, records):
    print(f'save_new_records_def')
 
    existing = load_existing_timestamps()
    new_records = [r for r in records if r["datetime_iso"] not in existing]
    if not new_records:
        print("No new records to save.")
        return

    existing_rows=[]
    with open(EARTHQUAKE_CSV_FILE, "r", newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        existing_rows = reader

    # Combine: new records first, then old ones
    all_rows = new_records + existing_rows
    
    # Create segments: Segment all the rows into temporal categories
    segment_earthquake_data(all_rows)

    # Write: Create the main earthquake csv
    with open(EARTHQUAKE_CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Saved {len(new_records)} new records.")

def run():
    html = fetch_html()
    headers, records = parse_table(html)
    save_new_records(headers, records)

if __name__ == "__main__":
    run()


    
        
