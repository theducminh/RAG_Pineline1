# utils/supabase_client.py
import os
from supabase import create_client
import dotenv

dotenv.load_dotenv()
def get_supabase_client():
    """
    Initialize and return a Supabase client using environment variables.
    """
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")  # Sử dụng Service Role Key để có quyền nạp dữ liệu và tạo vector
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Supabase URL and KEY must be set in environment variables.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)