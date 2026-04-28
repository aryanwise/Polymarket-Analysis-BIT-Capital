"""
utils/supabase_client.py

Shared Supabase client — import this instead of initializing
create_client() separately in every file.

Usage:
    from utils.supabase_client import get_service_client, get_anon_client

    # For scraper, filter, report, dig_deeper (write access)
    supabase = get_service_client()

    # For Streamlit frontend (read-only)
    supabase = get_anon_client()
"""
import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")  # write access
SUPABASE_ANON_KEY    = os.getenv("SUPABASE_ANON_KEY")     # read-only


def get_service_client() -> Client:
    """
    Service role client — full read/write access.
    Use in: scraper.py, filter.py, report.py, dig_deeper.py, prices.py
    Never expose this key to the frontend.
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise ValueError(
            "Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env"
        )
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def get_anon_client() -> Client:
    """
    Anon key client — read-only, safe to use in Streamlit frontend.
    Use in: streamlit_app.py
    """
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        raise ValueError(
            "Missing SUPABASE_URL or SUPABASE_ANON_KEY in .env"
        )
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)