"""
debug_gemini.py - Test Gemini API connection and single request
"""

import os
from dotenv import load_dotenv

# Load .env
load_dotenv()

print("=" * 60)
print("DEBUGGING GEMINI API CONNECTION")
print("=" * 60)

# Check API key
api_key = os.environ.get("GEMINI_API_KEY")
print(f"\n1. API Key found: {'YES' if api_key else 'NO'}")
if api_key:
    print(f"   First 10 chars: {api_key[:10]}...")
    print(f"   Length: {len(api_key)} chars")
else:
    print("   ❌ No API key found in environment variables")
    print("   Check your .env file exists and has: GEMINI_API_KEY=your_key_here")

# Try importing
print("\n2. Testing imports...")
try:
    from google import genai
    print("   ✓ google.genai imported successfully")
except ImportError as e:
    print(f"   ❌ Failed to import google.genai: {e}")
    print("   Run: uv pip install google-genai")

# Try creating client
print("\n3. Creating Gemini client...")
if api_key:
    try:
        client = genai.Client(api_key=api_key)
        print("   ✓ Client created successfully")
        
        # Test simple request
        print("\n4. Testing simple API call...")
        response = client.models.generate_content(
            model="gemini-flash-latest",
            contents="Say 'Hello World' in one word.",
        )
        print(f"   ✓ API call successful!")
        print(f"   Response: {response.text}")
        
    except Exception as e:
        print(f"   ❌ Error: {type(e).__name__}: {e}")
else:
    print("   ❌ Skipping - no API key")

print("\n" + "=" * 60)