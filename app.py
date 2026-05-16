import os
import asyncio
import time
import requests  # Handles communication for both Butterbase and TokenRouter APIs
import json
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError
import random

# Sponsor SDK Imports
from agentfield import Agent

# Import your local scrapper module
from scrapper import execute_local_ingestion

# Load local environment configurations (.env)
load_dotenv()

# -------------------------------------------------------------------------
# Sponsor Tools Configuration & Initialization
# -------------------------------------------------------------------------

# 1. Butterbase REST Configuration 
BUTTERBASE_API_KEY = os.getenv("BUTTERBASE_API_KEY", "Cheeks!")
BUTTERBASE_APP_ID = "app_sfb6qaijfhfc"
BUTTERBASE_TABLE_URL = f"https://api.butterbase.ai/v1/{BUTTERBASE_APP_ID}/tracked_products"

butterbase_headers = {
    "Authorization": f"Bearer {BUTTERBASE_API_KEY}",
    "Content-Type": "application/json",
}

# 2. TokenRouter REST Web API Configuration
TOKENROUTER_API_KEY = os.getenv('TOKENROUTER_API_KEY')
TOKENROUTER_URL = "https://api.tokenrouter.com/v1/chat/completions"

if not TOKENROUTER_API_KEY:
    raise RuntimeError('Missing TOKENROUTER_API_KEY in environment; set it in .env or environment variables')

def call_tokenrouter_api(model_target: str, prompt: str) -> str:
    """
    Executes a stateless HTTP POST request directly against the TokenRouter Web API gateway,
    handling model fallbacks and routing under the hood.
    """
    headers = {
        "Authorization": f"Bearer {TOKENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }

    # Payload format mapping to official TokenRouter interface routing documentation
    payload = {
        "model": model_target,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7
    }

    max_attempts = 3
    timeout_seconds = 45

    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(
                TOKENROUTER_URL,
                json=payload,
                headers=headers,
                timeout=timeout_seconds,
            )
            response.raise_for_status()

            # Process standard web completion response trees
            response_data = response.json()
            return response_data["choices"][0]["message"]["content"]
        except requests.exceptions.Timeout as exc:
            last_error = exc
            if attempt < max_attempts:
                time.sleep(0.5 * attempt)
                continue
            raise RuntimeError(
                f"TokenRouter request timed out after {max_attempts} attempts (timeout={timeout_seconds}s)"
            ) from exc
        except requests.exceptions.RequestException as exc:
            last_error = exc
            resp = getattr(exc, "response", None)
            if resp is not None and resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                time.sleep(0.5 * attempt)
                continue
            raise

    raise RuntimeError(f"TokenRouter request failed after retries: {last_error}")

# 3. AgentField Control Plane Configuration (Bound to Z.ai glm-5.1 Base Engine)
agent_node = Agent(
    node_id="arbitrage-sourcing-mesh",
    agentfield_server="http://localhost:8080",
    base_model="z-ai:glm-5.1",  # Configures Z.ai to drive the core reasoner math logic
    dev_mode=True
)

# -------------------------------------------------------------------------
# Pydantic Structural Data Modeling
# -------------------------------------------------------------------------
class ProductPayload(BaseModel):
    product_name: str = Field(..., min_length=1)
    retail_price: float = Field(..., gt=0)
    wholesale_price: float = Field(..., gt=0)
    potential_margin: float = Field(..., gt=0)

# -------------------------------------------------------------------------
# AgentField Core Reasoner Function Execution Loop
# -------------------------------------------------------------------------
@agent_node.reasoner(tags=["ecommerce", "arbitrage-hunting"])
async def process_arbitrage_opportunities(batch_input: dict) -> dict:
    """
    Main AgentField execution context powered by Z.ai. Evaluates ingested streams,
    routes creative listing requests to Qwen Cloud via the TokenRouter Web API, and stores to Butterbase.
    """
    print("\n" + "="*60)
    print("🤖 AGENTFIELD MULTI-AGENT MESH LOOP INITIALIZED (Powered by Z.ai)")
    print("="*60)
    
    # Ingest dynamic e-commerce data vectors from your custom scrapper module
    scraped_items = execute_local_ingestion()
    processed_count = 0
    profitable_insights = []

    for index, raw_item in enumerate(scraped_items, start=1):
        print(f"\n[Evaluator Task {index}/{len(scraped_items)}] Z.ai validating schema constraints...")
        
        try:
            item = ProductPayload(**raw_item)
        except ValidationError as ve:
            print(f"⚠️ Schema mismatch detected. Skipping item. Errors: {ve.json()}")
            continue

        # Execute Arbitrage Hunter Math Formula (Retail - Wholesale - Fees)
        estimated_marketplace_fees = 2.50
        net_profit_margin = item.potential_margin
        
        print(f"   🔍 Product Tracking Name: {item.product_name}")
        print(f"   📊 Metrics Grid -> Retail: ${item.retail_price:.2f} | Wholesale: ${item.wholesale_price:.2f}")
        print(f"   📐 Evaluated Net Profit Margin: ${net_profit_margin:.2f}")

        # Filtration boundary constraint logic (Only log high-yield opportunities >= $15.00)
        if net_profit_margin >= 15.00:
            print(f"   🔥 HIGH-MARGIN TARGET DETECTED. Passing to Qwen Cloud copywriter agent...")
            
            # Use TokenRouter Web API to target Qwen Cloud's flash/omni variants
            print("   🚦 Routing listing generation prompt to Qwen Cloud via TokenRouter Web API...")
            prompt_context = (
                f"Take this product name: '{item.product_name}' and retail price: ${item.retail_price:.2f}. "
                f"Write an optimized, high-converting eBay/Shopify product listing description."
            )
            
            try:
                # Call the direct Web API routing helper
                ai_marketing_copy = call_tokenrouter_api(
                    model_target="qwen/qwen3.5-flash", 
                    prompt=prompt_context
                )
                print("   ✅ Qwen Cloud Copywriter Agent generated listing description asset successfully.")
            except Exception as tr_err:
                print(f"   ⚠️ TokenRouter Web API routing error: {tr_err}.")
                resp = getattr(tr_err, "response", None)
                if resp is not None:
                    print(f"      -> Status: {resp.status_code} | Body: {resp.text}")
                print("      Using baseline fallback for marketing copy.")
                ai_marketing_copy = f"Exclusive hackathon deal on {item.product_name}! Buy now for just ${item.retail_price:.2f}."

            # Construct the relational data payload matching your Postgres schema types
            db_row_payload = {
                "id": random.randint(0, 2**31 - 1),
                "product_name": item.product_name,
                "retail_price": round(item.retail_price, 2),
                "wholesale_price": round(item.wholesale_price, 2),
                "potential_margin": round(net_profit_margin, 2),
                "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                # Adds marketing copy to database mapping if required by your schema definition
                # "marketing_copy": ai_marketing_copy
            }

            # Persist row record directly inside your Butterbase Cloud Instance via REST HTTP POST
            print("   🗄️ Transporting structured packet block directly to Butterbase REST Endpoint...")
            try:
                response = requests.post(
                    BUTTERBASE_TABLE_URL, 
                    json=db_row_payload, 
                    headers=butterbase_headers,
                    timeout=10
                )
                response.raise_for_status()
                print("   ✅ TRANSACTION ARCHIVED: Safely committed to Butterbase cloud tables.")
                
                processed_count += 1
                profitable_insights.append({
                    "product_name": item.product_name,
                    "potential_margin": round(net_profit_margin, 2),
                    "marketing_copy": ai_marketing_copy,
                })
                
            except Exception as db_err:
                resp = getattr(db_err, 'response', None)
                if resp is not None:
                    print(f"   ❌ Butterbase HTTP error {resp.status_code} -> Body: {resp.text}")
                else:
                    print(f"   ❌ Butterbase cloud persistence layer execution refused: {db_err}")
        else:
            print("   💤 MARGIN GAP BELOW CRITICAL VALUE: Pipeline filtering item to maximize efficiency.")

    print("\n" + "="*60)
    print("🏁 PIPELINE WORKFLOW LOOP EXECUTION COMPLETED")
    print("="*60 + "\n")
    
    return {
        "status": "success",
        "items_ingested": len(scraped_items),
        "profitable_deals_saved": processed_count,
        "summary": profitable_insights
    }

# -------------------------------------------------------------------------
# Main Application Execution Block
# -------------------------------------------------------------------------
if __name__ == "__main__":
    # Diagnostic TokenRouter network connectivity test before running core mesh
    def test_tokenrouter_connectivity():
        print('\n--- TokenRouter Web API Ping Test ---')
        try:
            test_response = call_tokenrouter_api(
                model_target="qwen/qwen3.5-flash",
                prompt="TokenRouter connection test ping. Verify routing handshake."
            )
            print('TokenRouter API Connection OK:', test_response)
        except Exception as e:
            print('TokenRouter API Connection failed:', e)
            resp = getattr(e, 'response', None)
            if resp is not None:
                print(f'  Status code: {resp.status_code} | Response payload: {resp.text}')
        print('--- end TokenRouter test ---\n')

    # test_tokenrouter_connectivity()

    # Test-execute the complete AgentField workflow locally
    asyncio.run(process_arbitrage_opportunities(batch_input={}))
