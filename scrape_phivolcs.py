import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime, timedelta
import os
import pandas as pd
from datetime import timedelta

URL = "https://earthquake.phivolcs.dost.gov.ph/"
CSV_FILE = "earthquakes.csv"

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
    if not os.path.exists(CSV_FILE):
        return set()
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["datetime_iso"] for row in reader}

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

    # Save to CSV Files
    output_files = {
        "earthquakes_past_hour.csv": past_hour,
        "earthquakes_today.csv": today,
        "earthquakes_past_24_hours.csv": past_24h,
        "earthquakes_past_7_days.csv": past_7d,
    }

    for filename, subset in output_files.items():
        subset.to_csv(filename, index=False)
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
    with open(CSV_FILE, "r", newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        existing_rows = reader

    # Combine: new records first, then old ones
    all_rows = new_records + existing_rows
    
    # Create segments: Segment all the rows into temporal categories
    segment_earthquake_data(all_rows)

    # Write: Create the main earthquake csv
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Saved {len(new_records)} new records.")

    segment_earthquake_data(all_rows)

def run():
    html = fetch_html()
    headers, records = parse_table(html)
    save_new_records(headers, records)

if __name__ == "__main__":
    run()


    
        
