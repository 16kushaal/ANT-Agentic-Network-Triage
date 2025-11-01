# in prep_data.py
import pandas as pd
import glob
import os

# --- 1. CONFIGURATION ---
DATA_DIR = './data/'
OUTPUT_FILE = os.path.join(DATA_DIR, 'CIC-2017_Shuffled_Combined_Transformed.csv')

# TODO: Fill this map with your 2017 -> 2018 column name changes
# 'Key' = 2017 Name, 'Value' = 2018 Name
# Example: {'Flow Pkts/s': 'Flow Packets/s', 'Bwd Pkt Len Max': 'Bwd Packet Length Max'}
CIC_2017_TO_2018_MAP = {
    # 'Flow Pkts/s': 'Flow Packets/s', # Example
    # ... Add all mappings here ...
}

# --- DEFINE YOUR FEATURE LISTS (from your prompt) ---
SUPERVISED_FEATURES = [
    'Init Fwd Win Byts', 'Fwd Seg Size Min', 'Dst Port', 'Fwd Header Len', 'Flow IAT Min', 
    'Flow Duration', 'Fwd IAT Max', 'Fwd IAT Min', 'Fwd Pkts/s', 'Flow Pkts/s', 
    'Fwd IAT Tot', 'Flow IAT Max', 'Fwd Pkt Len Mean', 'Fwd IAT Mean', 'Flow IAT Mean', 
    'Bwd Pkts/s', 'Pkt Len Mean', 'TotLen Fwd Pkts', 'Flow Byts/s', 'Init Bwd Win Byts', 
    'Bwd Pkt Len Mean', 'Bwd Pkt Len Std', 'Bwd Pkt Len Max', 'Pkt Len Var', 
    'Fwd Seg Size Avg', 'Tot Fwd Pkts', 'Bwd Header Len', 'Pkt Len Max', 
    'Subflow Fwd Byts', 'Subflow Fwd Pkts'
]

UNSUPERVISED_FEATURES = [
    'Init Fwd Win Byts', 'Down/Up Ratio', 'URG Flag Cnt', 'Dst Port',
    'Fwd Pkt Len Min', 'Fwd PSH Flags', 'Init Bwd Win Byts', 'Pkt Len Var',
    'RST Flag Cnt', 'Fwd Seg Size Min', 'Tot Bwd Pkts', 'Fwd Pkt Len Max',
    'Bwd Pkt Len Mean', 'Flow Byts/s', 'Bwd IAT Std', 'Fwd Header Len',
    'Bwd IAT Min', 'TotLen Fwd Pkts', 'Active Mean', 'ACK Flag Cnt',
    'Bwd Pkt Len Min', 'Active Std', 'Idle Min', 'Flow Pkts/s',
    'PSH Flag Cnt', 'Flow Duration', 'Fwd IAT Mean', 'Bwd IAT Mean',
    'Bwd Pkt Len Std', 'Tot Fwd Pkts'
]

# --- CREATE THE "MASTER" SCHEMA ---
# We use a set to get the unique union of all features
all_features_set = set(SUPERVISED_FEATURES) | set(UNSUPERVISED_FEATURES)
MASTER_SCHEMA_COLUMNS = sorted(list(all_features_set))

print(f"Master schema created with {len(MASTER_SCHEMA_COLUMNS)} unique features.")

# --- 2. SCRIPT ---

all_csv_files = glob.glob(os.path.join(DATA_DIR, '*.csv'))
all_csv_files = [f for f in all_csv_files if 'Shuffled' not in f]

if not all_csv_files:
    print(f"Error: No CSV files found in {DATA_DIR}")
else:
    print(f"Found {len(all_csv_files)} CSVs to process:")
    df_list = []
    for f in all_csv_files:
        try:
            # 1. Load the 2017 CSV
            df = pd.read_csv(f)
            print(f"Processing {f} ({len(df)} rows)...")
            
            # 2. Clean column names (strip spaces)
            df.columns = df.columns.str.strip()
            
            # 3. Rename 2017 columns to 2018 names
            df.rename(columns=CIC_2017_TO_2018_MAP, inplace=True)
            
            # 4. Align Schema to the new "MASTER" list
            #    This adds all columns needed by *either* model, filling with 0
            df = df.reindex(columns=MASTER_SCHEMA_COLUMNS, fill_value=0)
            
            df_list.append(df)
            
        except Exception as e:
            print(f"Error processing {f}: {e}")
            
    # 5. Combine all transformed DataFrames
    print("\nCombining all transformed data...")
    combined_df = pd.concat(df_list, ignore_index=True)
    print(f"Total combined rows: {len(combined_df)}")

    # 6. Shuffle
    print("Shuffling dataset...")
    shuffled_df = combined_df.sample(frac=1).reset_index(drop=True)

    # 7. Save the final file
    try:
        shuffled_df.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✅ Successfully transformed, shuffled, and saved data to:")
        print(f"   {OUTPUT_FILE}")
    except Exception as e:
        print(f"Error saving file: {e}")