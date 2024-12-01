import pandas as pd
from pathlib import Path
import constants  # Importing constants for directory paths
from urllib.parse import unquote  # To decode the '%3A' encoded characters
from pytz import timezone
from datetime import datetime
import pytz

pd.options.mode.copy_on_write = True


# Function to clean symbol names in mapping data
def clean_symbol(symbol):
    if symbol.endswith("-EQ"):
        return symbol[:-3]  # Remove the '-EQ' suffix
    return symbol



# Function to load and process OI data from nested folder structure
def process_oi_data_with_mapping(data_dir: str, mapping_data: pd.DataFrame, filter_symbols=None, filter_tokens=None):
    base_path = Path(data_dir)
    oi_data = []
    ist = pytz.timezone('Asia/Kolkata')

    # Iterate over each date directory
    for date_folder in base_path.iterdir():
        if date_folder.is_dir():  # Ensure it's a directory
            print(f"Processing date folder: {date_folder.name}")
            # Iterate over stock folders inside each date directory
            for stock_folder in date_folder.iterdir():
                if stock_folder.is_dir():
                    stock_symbol = stock_folder.name
                    
                    # Process all stocks if no filters are given
                    if filter_symbols is None and filter_tokens is None:
                        process_this_stock = True
                    else:
                        # Check if the stock symbol matches the filter criteria
                        process_this_stock = (
                            (filter_symbols and stock_symbol in filter_symbols) or
                            (filter_tokens and mapping_data[mapping_data['symbol'] == stock_symbol]['token'].isin(filter_tokens).any())
                        )
                    
                    if not process_this_stock:
                        # print(f"  Skipping stock folder: {stock_symbol}")
                        continue
                    
                    print(f"  Processing stock folder: {stock_symbol}")
                    # Process files in the stock folder
                    for file in stock_folder.glob("*.csv"):
                        # print(f"    Processing file: {file.name}")
                        try:
                            # Extract timestamp from the file name and decode the URL encoding
                            timestamp_str = file.stem  # Example: "2024-11-29T09%3A15%3A04.633"
                            decoded_timestamp = unquote(timestamp_str)  # Decode the '%3A' to ':'
                            
                            # Convert to a proper datetime format (optional step for further analysis)
                            timestamp = pd.to_datetime(decoded_timestamp, format="%Y-%m-%dT%H:%M:%S.%f")

                            timestamp_rounded = timestamp.replace(second=0, microsecond=0)
                            
                            # Convert to IST timezone
                            timestamp_ist = timestamp_rounded.tz_localize(ist)  # Convert to IST (Asia/Kolkata)
                            
                            # Format the timestamp in "YYYY-MM-DD HH:MM:00+05:30" format
                            timestamp_formatted = timestamp_ist.strftime("%Y-%m-%d %H:%M:00%z")

                            df = pd.read_csv(file)
                            # Verify the required columns are present
                            required_columns = [
                                "symbol", "openInterest", "buildUp", "ltp"
                            ]
                            if all(col in df.columns for col in required_columns):
                                filtered_df = df[required_columns]
                                
                                filtered_df[['Stock Name', 'Expiry', 'StrikePrice', 'Type']] = filtered_df['symbol'].str.extract(r"([A-Za-z]+)(\d{2}[A-Za-z]{3}\d{2})(\d+)(CE|PE)", expand=True)
                                del filtered_df['symbol']

                                # Append the corresponding symbol and token from mapping_data
                                stock_token = mapping_data.loc[
                                    mapping_data['symbol'] == stock_symbol, 'token'
                                ].values

                                if stock_token.size > 0:  # Check if a valid token exists
                                    # filtered_df['Stock Symbol'] = stock_symbol
                                    filtered_df['Token'] = stock_token[0]
                                    # Safely add the 'Timestamp' column using .loc to avoid chained indexing warning
                                    filtered_df.loc[:, 'Time'] = timestamp_formatted   # Avoid the "view vs copy" warning
                                    oi_data.append(filtered_df)
                                else:
                                    print(f"    No matching token found for stock: {stock_symbol}")
                            else:
                                print(f"    Missing required columns in file: {file.name}")
                        except Exception as e:
                            print(f"    Error processing file {file.name}: {e}")

    if not oi_data:
        print("No valid data found in the specified directory.")
        return pd.DataFrame()  # Return an empty DataFrame to avoid concatenation error

    # Combine data from all files
    
    combined_oi = pd.concat(oi_data, ignore_index=True)
    return combined_oi

# File paths and mapping data
data_dir = constants.data_dir  # Load from constants
mapping_file = constants.mapping_file  # Load from constants

# Load and clean mapping data
def load_mapping(mapping_file: str):
    mapping_data = pd.read_csv(mapping_file)
    # Clean the 'symbol' column
    mapping_data['symbol'] = mapping_data['symbol'].apply(clean_symbol)
    return mapping_data

mapping_data = load_mapping(mapping_file)

# Filter criteria from constants (optional)
filter_symbols = getattr(constants, "filter_symbols", None)  # Default to None if not defined
filter_tokens = getattr(constants, "filter_tokens", None)  # Default to None if not defined

# Process nested OI data
oi_data = process_oi_data_with_mapping(data_dir, mapping_data, filter_symbols, filter_tokens)

# Display or save the enriched OI data
if not oi_data.empty:
    print(oi_data)
    oi_data.to_csv("./Output/All_stock_open_interest_data.csv", index=False)
else:
    print("No data to save.")
