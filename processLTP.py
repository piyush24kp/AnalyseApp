import os
import pandas as pd
from urllib.parse import unquote
import constants  # For directory paths and filter criteria
from pytz import timezone

# File paths and mapping data
data_dir = constants.ltp_data_dir
mapping_file = constants.mapping_file
filter_symbols = constants.filter_symbols  # List of symbols to process
filter_tokens = constants.filter_tokens  # List of tokens to process

def process_ltp_data_with_ohlc(data_dir, mapping_file, filter_symbols=None, filter_tokens=None):
    """
    Processes LTP data from the given directory, calculates OHLC, and applies optional filters for symbols and tokens.
    """
    print(f"Loading mapping file: {mapping_file}")
    try:
        mapping_df = pd.read_csv(mapping_file)
        mapping_df["symbol"] = mapping_df["symbol"].str.replace("-EQ", "")  # Clean up symbols
        print("Mapping file loaded successfully.")
    except Exception as e:
        print(f"Failed to load mapping file: {e}")
        return pd.DataFrame()

    combined_data = []

    # Traverse the data directory
    print(f"Processing LTP data from directory: {data_dir}")
    for date_folder in os.listdir(data_dir):
        date_path = os.path.join(data_dir, date_folder)
        if not os.path.isdir(date_path):
            print(f"Skipping non-directory entry: {date_path}")
            continue

        option_chain_path = os.path.join(date_path, "options-chain")
        if not os.path.exists(option_chain_path):
            print(f"Option-chain folder not found in: {date_path}")
            continue

        print(f"Processing date folder: {date_folder}")
        for file_name in os.listdir(option_chain_path):
            file_path = os.path.join(option_chain_path, file_name)
            if not file_name.endswith(".csv"):
                print(f"Skipping non-CSV file: {file_name}")
                continue

            # Extract date and time from the file name
            decoded_name = unquote(file_name)
            timestamp = decoded_name.replace(".csv", "")

            try:
                ltp_df = pd.read_csv(file_path)
            except Exception as e:
                print(f"Failed to read file {file_path}: {e}")
                continue

            # Filter based on tokens or symbols if provided
            if filter_tokens:
                ltp_df = ltp_df[ltp_df["token"].isin(filter_tokens)]
            if filter_symbols:
                matching_tokens = mapping_df[mapping_df["symbol"].isin(filter_symbols)]["token"].tolist()
                ltp_df = ltp_df[ltp_df["token"].isin(matching_tokens)]

            if ltp_df.empty:
                print(f"No data left after filtering for file: {file_name}")
                continue

            # Add Stock Symbol and Date columns
            ltp_df = pd.merge(ltp_df, mapping_df[["symbol", "token"]], on="token", how="left")
            ltp_df["Date"] = date_folder
            ltp_df["Timestamp"] = timestamp

            # Calculate OHLC for each token
            ohlc_df = ltp_df.groupby("token").agg(
                Open=("ltp", "first"),
                High=("ltp", "max"),
                Low=("ltp", "min"),
                Close=("ltp", "last"),
            ).reset_index()

            # Merge OHLC data back into the main DataFrame
            ltp_df = pd.merge(ltp_df, ohlc_df, on="token", how="left")

            # Append processed data
            combined_data.append(ltp_df)

    # Combine all processed data into a single DataFrame
    if combined_data:
        result_df = pd.concat(combined_data, ignore_index=True)
        print("All files processed successfully.")
    else:
        print("No data files processed.")
        result_df = pd.DataFrame(columns=["token", "time", "ltp", "volume", "symbol", "Date", "Timestamp", "Open", "High", "Low", "Close"])

    return result_df


def group_by_five_minute_intervals_with_ist(df):
    """
    Groups the data into 5-minute intervals and calculates OHLC for each group.
    The final timestamps are converted to IST.
    """
    # Define IST timezone
    ist = timezone("Asia/Kolkata")

    # Convert 'time' from milliseconds to datetime and localize to UTC
    df['time'] = pd.to_datetime(df['time'], unit='ms').dt.tz_localize('UTC')

    # Convert time to IST
    df['time'] = df['time'].dt.tz_convert(ist)

    # Create a new column for 5-minute interval start time (in IST)
    df['5min_interval'] = df['time'].dt.floor('5min')

    # Convert the 5min_interval to IST (this step ensures that 5min_interval is in IST)
    df['5min_interval'] = df['5min_interval'].dt.tz_localize(None)  # Remove UTC time zone info
    df['5min_interval'] = pd.to_datetime(df['5min_interval']).dt.tz_localize(ist)  # Add IST timezone

    # Group by token and 5-minute interval, then calculate OHLC and other aggregations
    grouped = df.groupby(['token', '5min_interval']).agg(
        Open=('ltp', 'first'),  # First LTP in the interval
        High=('ltp', 'max'),    # Highest LTP in the interval
        Low=('ltp', 'min'),     # Lowest LTP in the interval
        Close=('ltp', 'last'),  # Last LTP in the interval
        Volume=('volume', 'sum'),  # Total volume in the interval
        Symbol=('symbol', 'first'),  # Symbol remains the same for each group
        Date=('Date', 'first')  # Date remains the same for each group
    ).reset_index()

    return grouped


# Example Usage
if __name__ == "__main__":
    df = process_ltp_data_with_ohlc(data_dir, mapping_file, filter_symbols=filter_symbols, filter_tokens=filter_tokens)
    ltp_data_with_ohlc = group_by_five_minute_intervals_with_ist(df)
    ltp_data_with_ohlc.to_csv("./Output/All_stocks_ltp_ohlc_data.csv", index=False)
    print("LTP data with OHLC processed and saved to 'processed_ltp_ohlc_data.csv'.")
