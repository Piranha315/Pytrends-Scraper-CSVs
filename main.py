import pandas as pd
from pytrends.request import TrendReq
import time
import os
import random
from datetime import datetime, timedelta

 
# 1. HELPER FUNCTIONS 
def chunker(seq, size):
    for pos in range(0, len(seq), size):
        yield seq[pos:pos + size]

def get_pytrends_session(proxy_url):
    proxies = [proxy_url] if proxy_url else None
    return TrendReq(hl='en-US', tz=360, timeout=(15,30), proxies=proxies, retries=3)

def load_processed_cards(checkpoint_file):
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            return set(line.strip() for line in f)
    return set()

def format_time(seconds):
    return str(timedelta(seconds=int(seconds)))

#  CONFIGURATION 
password = '{Your_Password}'
username = 'spx0gsp2ks'
PROXY_URL = f"http://{username}:{password}@{Your_Proxy_Provider}" 
OUTPUT_FILE = 'mtg_trends_master.csv'
CHECKPOINT_FILE = 'processed_cards.txt'

#  DATA PREPARATION 
df_input = pd.read_csv('{YOUR_CSV}')
all_cards = df_input.iloc[:, 0].dropna().drop_duplicates().tolist()
processed = load_processed_cards(CHECKPOINT_FILE)
cards_to_process = [c for c in all_cards if c not in processed]

total_to_do = len(cards_to_process)
total_done_this_session = 0
start_time = time.time()

print(f"Total Unique Cards: {len(all_cards)}")
print(f"Already Processed: {len(processed)}")
print(f"Remaining to Scrape: {total_to_do}\n")

pytrends = get_pytrends_session(PROXY_URL)

# 4. MAIN PROCESS LOOP
for i, batch in enumerate(chunker(cards_to_process, 5)):
    batch_start = time.time()
    try:
        pytrends.build_payload(batch, timeframe='today 5-y')
        df = pytrends.interest_over_time()
        
        if not df.empty:
            if 'isPartial' in df.columns:
                df = df.drop(columns=['isPartial'])
            
            df = df.reset_index()
            df_melted = df.melt(id_vars=['date'], var_name='colname', value_name='interest')
            
            file_exists = os.path.isfile(OUTPUT_FILE)
            df_melted.to_csv(OUTPUT_FILE, mode='a', index=False, header=not file_exists)
            
            with open(CHECKPOINT_FILE, 'a') as f:
                for card in batch:
                    f.write(f"{card}\n")
            
            total_done_this_session += len(batch)

        # PROGRESS TRACKING LOGIC 
        elapsed = time.time() - start_time
        avg_time_per_card = elapsed / total_done_this_session if total_done_this_session > 0 else 0
        remaining_cards = total_to_do - total_done_this_session
        etr_seconds = avg_time_per_card * remaining_cards
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] "
              f"Progress: {total_done_this_session}/{total_to_do} | "
              f"Elapsed: {format_time(elapsed)} | "
              f"ETR: {format_time(etr_seconds)}")

        # Jittered sleep
        time.sleep(random.uniform(6, 12)) 

    except Exception as e:
        print(f"Error on batch {batch}: {e}")
        if "429" in str(e):
            cooldown = random.uniform(300, 600)
            print(f"Rate limited! Cooldown: {int(cooldown/60)}m...")
            time.sleep(cooldown)
            pytrends = get_pytrends_session(PROXY_URL)
        else:
            time.sleep(random.uniform(10, 20))
            continue

print(f"\nSuccess! Total session time: {format_time(time.time() - start_time)}")
