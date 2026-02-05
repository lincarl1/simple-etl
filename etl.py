import json
import pandas as pd

# -----------------------
# CONFIGURATION
# -----------------------
RAW_EVENTS_PATH = './data/raw_events.json'
USERS_PATH = './data/users.csv'
OUTPUT_DIR = './output'
CLEAN_EVENTS_PATH = f'{OUTPUT_DIR}/clean_events.parquet'
DAILY_SUMMARY_PATH = f'{OUTPUT_DIR}/daily_summary.parquet'

# -----------------------
# EXTRACT
# -----------------------
def load_json_df(path):
    """Load JSON files into a DataFrame
    
    Args:
        path (str): Path of JSON file

    Returns:
        dataframe: JSON dataframe
    """
    with open(path, "r") as f:
        data = json.load(f)

    df = pd.json_normalize(data)
    return df


def load_csv_df(path):
    """Load CSV files into a DataFrame
    
    Args:
        path (str): Path of CSV file

    Returns:
        dataframe: CSV dataframe
    """
    return pd.read_csv(path)


# -----------------------
# TRANSFORM
# -----------------------
def clean_events(df):
    """Cleans raw events dataframe

    We clean by doing the following:
    - Drop rows missing user_id
    - Parse timestamps and drop invalid ones
    - Deduplicate (ignore metadata columns)

    Args:
        df (dataframe): Events dataframe

    Returns:
        dataframe: Cleaned events dataframe
    """
    # Retrieve Initial Count
    initial_count = len(df)

    # Drop missing user_id
    missing_user_id_count = df["user_id"].isna().sum()
    df = df.dropna(subset=["user_id"])

    # Parse timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    invalid_timestamps_count = df["timestamp"].isna().sum()
    df = df.dropna(subset=["timestamp"])

    # Deduplicate ignoring metadata fields
    metadata_cols = [c for c in df.columns if c.startswith("metadata.")]
    dedupe_cols = [c for c in df.columns if c not in metadata_cols]

    before_dedupe = len(df)
    df = df.drop_duplicates(subset=dedupe_cols)
    deduped_count = before_dedupe - len(df)

    print(f"Initial row count: {initial_count}")
    print(f"Rows dropped due to bad users: {missing_user_id_count}")
    print(f"Rows dropped due to invalid timestamps: {invalid_timestamps_count}")
    print(f"Rows dropped due to duplicates: {deduped_count}")
    print(f"Remaining rows after cleaning: {len(df)}")

    return df


def join_and_filter(events, users):
    """Joins events with users and filter to US users only

    Args:
        events (dataframe): Events dataframe
        users (dataframe): Users dataframe

    Returns:
        dataframe: Enriched dataframe
    """
    joined = events.merge(users, on="user_id", how="inner")

    before_filter = len(joined)
    joined = joined[joined["country"] == "US"]
    filtered = before_filter - len(joined)

    print(f"Rows dropped due to non-US filter: {filtered}")
    print(f"Row count after join + filter: {len(joined)}")

    # Edge Case
    ## We should convert str type to datetime type for better efficiency
    ## I would've considered removing the bad input here, but when I checked it looked like all values were valid
    joined["signup_date"] = pd.to_datetime(joined["signup_date"], errors="coerce")

    return joined


def create_daily_summary(df):
    """Create daily aggregate dataframe
    
    Definition: Number of events per user per date

    Args:
        df (dataframe): Enriched dataframe for daily aggregate

    Returns:
        dataframe: Aggregated dataframe
    """
    df["event_date"] = df["timestamp"].dt.date

    daily_summary = (
        df.groupby(["event_date", "user_id"])
        .size()
        .reset_index(name="event_count")
    )

    return daily_summary

# -----------------------
# LOAD
# -----------------------
def write_parquet(df, path):
    """Write DataFrame to Parquet with Snappy compression
    
    Args:
        df (dataframe): Dataframe to be written to parquet
        path (str): Path of parquet file

    Returns:
        None
    """
    df.to_parquet(path, engine="pyarrow", compression="snappy")
    print(f"Wrote {len(df)} rows to {path}")

# -----------------------
# MAIN
# -----------------------
def main():
    """Program Driver Function

    Args:
        None

    Returns:
        None
    """
    print("ETL pipeline starting...")

    # Extract
    events = load_json_df(RAW_EVENTS_PATH)
    users = load_csv_df(USERS_PATH)

    # Transform
    clean = clean_events(events)
    enriched = join_and_filter(clean, users)
    daily_summary = create_daily_summary(enriched)

    # Load
    write_parquet(enriched, CLEAN_EVENTS_PATH)
    write_parquet(daily_summary, DAILY_SUMMARY_PATH)

    print("ETL pipeline completed successfully")

if __name__ == "__main__":
    main()