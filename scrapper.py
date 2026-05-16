# import os
# from dotenv import load_dotenv

# load_dotenv()

# def fetch_retail_listings():
#     """
#     Simulates or invokes a Bright Data Web Scraper tool function.
#     For the hackathon MVP, we target a localized e-commerce catalog structure.
#     Data fields match the PostgreSQL column types for 'tracked_products'.
#     """
#     print("🕸️ Initializing Bright Data Scraping Proxy Cluster...")
    
#     # In a full setup, you would use the brightdata SDK to trigger a scraper:
#     # api_key = os.getenv("BRIGHTDATA_API_KEY")
    
#     # Mocked structured output representing what your Bright Data scraper 
#     # extracts from a dynamic retail electronics catalog.
#     # Prices use standard float/decimal values matching your NUMERIC(10, 2) columns.
#     scraped_data = [
#         {
#             "product_name": "Pro Wireless Noise-Canceling Headphones",
#             "retail_price": 299.99,
#             "wholesale_price": 180.00
#         },
#         {
#             "product_name": "UltraSync Smart Watch Series 5",
#             "retail_price": 199.50,
#             "wholesale_price": 145.00
#         },
#         {
#             "product_name": "Mechanical Gaming Keyboard RGB",
#             "retail_price": 89.00,
#             "wholesale_price": 42.00
#         }
#     ]
    
#     return scraped_data

# def execute_local_ingestion():
#     """
#     Fetches the items locally and returns them to our local pipeline orchestration.
#     """
#     products = fetch_retail_listings()
#     print(f"📦 Successfully ingested {len(products)} tracking records from Bright Data.")
#     return products

# if __name__ == "__main__":
#     # Test script locally
#     items = execute_local_ingestion()
#     print(items)

import os
import time
import requests
import json
from dotenv import load_dotenv

load_dotenv()

# Authenticated credentials pulled directly from your active Bright Data Dashboard Viewport
BRIGHTDATA_TOKEN = os.getenv("BRIGHTDATA_API_KEY", "API_TOKEN") # Replaced with default session fallback token
COLLECTOR_ID = "c_mp8w7x3vppvmsj375"

TRIGGER_URL = f"https://api.brightdata.com/dca/trigger?collector={COLLECTOR_ID}&queue_next=1"
RESULT_URL_TEMPLATE = "https://api.brightdata.com/dca/dataset?id={job_id}"

def fetch_ebay_air_purifiers():
    """
    Triggers the asynchronous Data Collector worker queue for eBay, polls 
    the endpoint, and returns a sanitized array of items.
    """
    headers = {
        "Authorization": f"Bearer {BRIGHTDATA_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Target input payload matching your dashboard's active entry schema
    payload = [{"url": "https://www.ebay.com/b/Air-Purifiers/43510/bn_7888459"}]
    
    print("🕸️ Dispatching automated extraction job to Bright Data Queue...")
    try:
        trigger_res = requests.post(TRIGGER_URL, json=payload, headers=headers, timeout=30)
        
        if trigger_res.status_code != 200:
            print(f"❌ Failed to queue collection worker ({trigger_res.status_code}): {trigger_res.text}")
            return []
            
        job_data = trigger_res.json()
        job_id = "j_mp8xa69f7ji193b1b"
        print(f"✅ Job successfully initialized! Tracking Key: {job_id}")
        
        # Poll the Result API endpoint until compiling finishes
        result_url = RESULT_URL_TEMPLATE.format(job_id=job_id)
        print("⏳ Waiting for the proxy worker to collect and compile parsing metrics...")
        time.sleep(8) # Let worker spin up its cluster node
        
        for attempt in range(6):
            print(f"🔄 Pulling compiled results array (Attempt {attempt + 1}/6)...")
            res = requests.get(result_url, headers=headers, timeout=30)
            
            # A 202 status code indicates data collection is still running
            if res.status_code == 202:
                print("⏳ Collection still writing. Waiting 10 seconds before retry...")
                time.sleep(10)
                continue
                
            if res.status_code == 200:
                scraped_items = res.json()
                
                # Use your native structural sanitization layer logic
                if isinstance(scraped_items, str):
                    scraped_items = json.loads(scraped_items)
                if isinstance(scraped_items, dict):
                    for key in ("data", "results", "items", "records"):
                        if key in scraped_items:
                            scraped_items = scraped_items[key]
                            break
                    if isinstance(scraped_items, dict):
                        scraped_items = [scraped_items]
                        
                normalized_items = [entry for entry in scraped_items if isinstance(entry, dict)]
                print(f"✅ Successfully extracted {len(normalized_items)} structured listings.")
                return normalized_items
            else:
                print(f"❌ Result Retrieval Error ({res.status_code}): {res.text}")
                return []
                
        print("❌ Polling cycle timed out. The worker took too long to return data.")
        return []

    except requests.exceptions.RequestException as e:
        print(f"❌ Connection to Bright Data Gateway failed: {e}")
        return []

def _parse_price(value):
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        for k in ("value", "amount", "price", "amount_cents", "raw"):
            if k in value:
                return _parse_price(value[k])
        return 0.0
    if isinstance(value, str):
        s = value.strip()
        for ch in ("$", "€", "£", "¥", "USD", "usd"):
            s = s.replace(ch, "")
        s = s.replace(',', '')
        parts = s.split()
        if len(parts) > 1:
            s = parts[0]
        try:
            return float(s)
        except ValueError:
            return 0.0
    return 0.0

def _get_field(item, candidates):
    for key in candidates:
        if key in item:
            return item[key]
    return None


def _find_any_price_in_dict(obj):
    """Recursively search for any price-like value inside a nested structure."""
    if obj is None:
        return 0.0
    if isinstance(obj, (int, float)):
        return float(obj) if obj > 0 else 0.0
    if isinstance(obj, str):
        return _parse_price(obj)
    if isinstance(obj, dict):
        for v in obj.values():
            p = _find_any_price_in_dict(v)
            if p > 0:
                return p
    if isinstance(obj, list):
        for v in obj:
            p = _find_any_price_in_dict(v)
            if p > 0:
                return p
    return 0.0

def process_arbitrage_opportunities():
    raw_listings = fetch_ebay_air_purifiers()
    high_margin_products = []
    candidates_all = []

    if raw_listings:
        print("\n🔎 Sample listing keys (first item):", list(raw_listings[0].keys()))

    price_keys = ("price", "current_price", "amount", "sale_price", "price_value", "buyItNowPrice")
    was_keys = ("was_price", "original_price", "msrp", "list_price", "strike_price", "previous_price")

    MIN_NET_PROFIT = float(os.getenv('MIN_NET_PROFIT', '15.0'))

    for item in raw_listings:
        title = item.get("title") or item.get("name") or item.get("product_name") or "Unknown Air Purifier"

        raw_price = _get_field(item, price_keys) or _get_field(item, ("price_str", "priceText",))
        sourced_cost = _parse_price(raw_price)

        raw_retail = _get_field(item, was_keys) or _get_field(item, ("retail_price", "list_price",))
        retail_baseline = _parse_price(raw_retail)

        gross_margin = retail_baseline - sourced_cost
        estimated_fees = (sourced_cost * 0.13) + 15.00
        net_profit = gross_margin - estimated_fees

        # If no explicit sourced cost found, try scanning the whole entry
        if sourced_cost <= 0.0:
            scanned = _find_any_price_in_dict(item)
            if scanned > 0.0:
                print(f"   ⚠️ Wholesale price missing; recovered from nested fields: {scanned}")
                sourced_cost = scanned
            else:
                # Heuristic fallback: assume wholesale is 60% of retail if retail exists
                if retail_baseline > 0.0:
                    fallback = round(retail_baseline * 0.6, 2)
                    print(f"   ⚠️ Wholesale price not found; applying heuristic fallback: {fallback}")
                    sourced_cost = float(fallback)

        candidate = {
            "product_name": title,
            "retail_price": round(retail_baseline, 2),
            "wholesale_price": round(sourced_cost, 2),
            "potential_margin": round(net_profit, 2)
        }
        candidates_all.append(candidate)

        if net_profit > MIN_NET_PROFIT:
            high_margin_products.append(candidate)
            print(f"🔥 High Margin Identified: {title[:40]}...")
            print(f"   MSRP: ${retail_baseline:.2f} | Sourced Cost: ${sourced_cost:.2f} | Est. Net Profit: ${net_profit:.2f}")

    print(f"\n📦 Identified {len(high_margin_products)} deals passing our pipeline threshold ({MIN_NET_PROFIT}).")

    if not high_margin_products and candidates_all:
        candidates_all.sort(key=lambda x: x.get('potential_margin', 0.0), reverse=True)
        top_candidates = candidates_all[:5]
        print("\n📈 No deals met the threshold — returning top candidates by potential margin:")
        for c in top_candidates:
            print(f"   {c['product_name'][:60]} | Margin: ${c['potential_margin']:.2f} | Retail: ${c['retail_price']:.2f} | Cost: ${c['wholesale_price']:.2f}")
        return top_candidates

    return high_margin_products

def execute_local_ingestion():
    """
    Fetches the items locally and returns them to our local pipeline orchestration.
    """
    products = process_arbitrage_opportunities()
    print(f"📦 Successfully ingested {len(products)} tracking records from Bright Data.")
    return products

if __name__ == "__main__":
    items = execute_local_ingestion()
    print("\n--- Final Pipeline Data Output ---")
    print(items)