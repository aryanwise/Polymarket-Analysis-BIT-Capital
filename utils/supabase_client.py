"""
Shared Supabase client initialization.
Import this in all modules that need DB access.
"""
import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")


def get_service_client() -> Client:
    """Client with service_role key — for ingestion, filtering, reports."""
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def get_anon_client() -> Client:
    """Client with anon key — for Streamlit (read-only from browser)."""
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
