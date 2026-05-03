from utils.supabase_client import get_service_client
import pandas as pd

supabase = get_service_client()

# Fetch summary of markets by category
res = supabase.table("markets").select("id, volume, events(category)").execute()
df = pd.json_normalize(res.data)

print("--- Data Summary ---")
print(f"Total Markets: {len(df)}")
print("\n--- Markets per Category ---")
print(df['events.category'].value_counts())