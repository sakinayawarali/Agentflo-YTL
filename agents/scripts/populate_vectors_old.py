"""
Vector population script for existing Engro_Products_Structured table
Uses skucode as primary key and skudescription as the text field
"""

import os
import openai
from supabase import create_client, Client
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration - add these to your .env file
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") 
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Validate environment variables
if not SUPABASE_URL or not SUPABASE_KEY or not OPENAI_API_KEY:
    missing = []
    if not SUPABASE_URL: missing.append("SUPABASE_URL")
    if not SUPABASE_KEY: missing.append("SUPABASE_KEY") 
    if not OPENAI_API_KEY: missing.append("OPENAI_API_KEY")
    raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

def populate_vectors():
    """Populate vectors for products that don't have them"""
    
    # Initialize clients
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)
    
    print("🚀 Starting vector population with OpenAI embeddings...")
    
    # Get all products without vectors (using skucode as identifier)
    try:
        response = supabase.table('Engro_Products_Structured').select('skucode, skudescription').is_('sku_vector', 'null').execute()
        products = response.data
    except Exception as e:
        print(f"❌ Error fetching products: {e}")
        return False
    
    if not products:
        print("✅ All products already have vectors!")
        return True
    
    print(f"Found {len(products)} products to process")
    
    processed = 0
    failed = 0
    
    for product in products:
        try:
            # Generate embedding using OpenAI text-embedding-3-small
            embedding_response = openai_client.embeddings.create(
                input=product['skudescription'],  # Using skudescription
                model="text-embedding-3-small",
                dimensions=512
            )
            
            vector = embedding_response.data[0].embedding
            
            # Update in database using skucode as identifier
            supabase.table('Engro_Products_Structured').update({
                'sku_vector': vector
            }).eq('skucode', product['skucode']).execute()
            
            processed += 1
            print(f"✅ {processed}/{len(products)}: {product['skudescription']}")
            
            # Small delay to avoid rate limiting
            time.sleep(0.1)
            
        except Exception as e:
            failed += 1
            print(f"❌ Error with {product['skudescription']}: {e}")
            continue
    
    print(f"🎉 Vector population completed!")
    print(f"   Successfully processed: {processed}")
    print(f"   Failed: {failed}")
    
    return processed > 0

def create_vector_index():
    """Create the vector similarity index"""
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("🔧 Creating vector similarity index...")
    try:
        supabase.query("""
            CREATE INDEX IF NOT EXISTS idx_engro_sku_vector 
            ON "Engro_Products_Structured" 
            USING ivfflat (sku_vector vector_cosine_ops) 
            WITH (lists = 100);
        """).execute()
        print("✅ Vector index created!")
        return True
    except Exception as e:
        print(f"⚠️ Vector index creation failed: {e}")
        print("You'll need to create it manually in Supabase SQL editor:")
        print("""
        CREATE INDEX idx_engro_sku_vector ON "Engro_Products_Structured" 
        USING ivfflat (sku_vector vector_cosine_ops) WITH (lists = 100);
        """)
        return False

if __name__ == "__main__":
    print("📋 Starting vector population...")
    print("Prerequisites:")
    print("✓ Table 'Engro_Products_Structured' exists")
    print("✓ Columns: skucode (PK), skudescription, details, sku_vector")
    print("✓ Environment variables set in .env")
    print()
    
    # Step 1: Populate vectors
    if populate_vectors():
        print("\n" + "=" * 50)
        # Step 2: Create index
        create_vector_index()
        print("\n🎉 Setup complete! Ready for semantic search!")
    else:
        print("❌ Vector population failed. Check your table structure and data.")