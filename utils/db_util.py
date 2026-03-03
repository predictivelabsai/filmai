"""
Database utility functions for Film AI
Provides reusable database operations for scripts, actors, placements, and forecasts
Uses PostgreSQL via DB_URL with the 'filmai' schema.
"""

import os
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL")
SCHEMA = "filmai"


def get_connection():
    """
    Get PostgreSQL database connection.

    Returns:
        psycopg2 connection object
    """
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    return conn


def init_database() -> bool:
    """
    Initialize database with schema from sql/schema.sql

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sql', 'schema.sql')

        if not os.path.exists(schema_path):
            print(f"Schema file not found: {schema_path}")
            return False

        with open(schema_path, 'r') as f:
            schema_sql = f.read()

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(schema_sql)
        conn.commit()
        conn.close()

        return True

    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        return False


# ==================== SCRIPTS OPERATIONS ====================

def create_script(title: str, genre: str, content: str) -> Optional[int]:
    """
    Create a new script in the database

    Args:
        title: Script title
        genre: Script genre
        content: Script content

    Returns:
        int: Script ID if successful, None otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            INSERT INTO {SCHEMA}.scripts (title, genre, content, created_at, modified_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (title, genre, content, datetime.now(), datetime.now()))

        script_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()

        return script_id

    except Exception as e:
        print(f"Error creating script: {str(e)}")
        return None


def get_script(script_id: int) -> Optional[Dict[str, Any]]:
    """
    Get script by ID

    Args:
        script_id: Script ID

    Returns:
        dict: Script data or None if not found
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(f"SELECT * FROM {SCHEMA}.scripts WHERE id = %s", (script_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return None

    except Exception as e:
        print(f"Error getting script: {str(e)}")
        return None


def get_all_scripts() -> List[Dict[str, Any]]:
    """
    Get all scripts from database

    Returns:
        list: List of script dictionaries
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(f"SELECT * FROM {SCHEMA}.scripts ORDER BY created_at DESC")
        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    except Exception as e:
        print(f"Error getting scripts: {str(e)}")
        return []


def get_scripts_by_genre(genre: str) -> List[Dict[str, Any]]:
    """
    Get scripts filtered by genre

    Args:
        genre: Genre to filter by

    Returns:
        list: List of script dictionaries
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(f"SELECT * FROM {SCHEMA}.scripts WHERE genre = %s ORDER BY created_at DESC", (genre,))
        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    except Exception as e:
        print(f"Error getting scripts by genre: {str(e)}")
        return []


def update_script(script_id: int, title: Optional[str] = None,
                 genre: Optional[str] = None, content: Optional[str] = None) -> bool:
    """
    Update script fields

    Args:
        script_id: Script ID
        title: New title (optional)
        genre: New genre (optional)
        content: New content (optional)

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        updates = []
        params = []

        if title is not None:
            updates.append("title = %s")
            params.append(title)

        if genre is not None:
            updates.append("genre = %s")
            params.append(genre)

        if content is not None:
            updates.append("content = %s")
            params.append(content)

        if not updates:
            return False

        updates.append("modified_at = %s")
        params.append(datetime.now())
        params.append(script_id)

        sql = f"UPDATE {SCHEMA}.scripts SET {', '.join(updates)} WHERE id = %s"
        cursor.execute(sql, params)

        conn.commit()
        conn.close()

        return True

    except Exception as e:
        print(f"Error updating script: {str(e)}")
        return False


def delete_script(script_id: int) -> bool:
    """
    Delete script and all related data

    Args:
        script_id: Script ID

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Delete related records first
        cursor.execute(f"DELETE FROM {SCHEMA}.product_placements WHERE script_id = %s", (script_id,))
        cursor.execute(f"DELETE FROM {SCHEMA}.script_casting WHERE script_id = %s", (script_id,))
        cursor.execute(f"DELETE FROM {SCHEMA}.revenue_forecasts WHERE script_id = %s", (script_id,))
        cursor.execute(f"DELETE FROM {SCHEMA}.scripts WHERE id = %s", (script_id,))

        conn.commit()
        conn.close()

        return True

    except Exception as e:
        print(f"Error deleting script: {str(e)}")
        return False


# ==================== PRODUCT PLACEMENTS OPERATIONS ====================

def create_product_placement(script_id: int, product_name: str, brand: str,
                            placement_type: Optional[str] = None,
                            scene_description: Optional[str] = None,
                            estimated_cost: Optional[float] = None) -> Optional[int]:
    """
    Create a new product placement

    Args:
        script_id: Associated script ID
        product_name: Product name
        brand: Brand name
        placement_type: Type of placement (optional)
        scene_description: Scene description (optional)
        estimated_cost: Estimated cost (optional)

    Returns:
        int: Placement ID if successful, None otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            INSERT INTO {SCHEMA}.product_placements
            (script_id, product_name, brand, placement_type, scene_description, estimated_cost)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (script_id, product_name, brand, placement_type, scene_description, estimated_cost))

        placement_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()

        return placement_id

    except Exception as e:
        print(f"Error creating product placement: {str(e)}")
        return None


def get_placements_by_script(script_id: int) -> List[Dict[str, Any]]:
    """
    Get all product placements for a script

    Args:
        script_id: Script ID

    Returns:
        list: List of placement dictionaries
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(f"SELECT * FROM {SCHEMA}.product_placements WHERE script_id = %s", (script_id,))
        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    except Exception as e:
        print(f"Error getting placements: {str(e)}")
        return []


# ==================== ACTORS OPERATIONS ====================

def create_actor(tmdb_id: int, name: str, country: Optional[str] = None,
                popularity: Optional[float] = None,
                profile_path: Optional[str] = None) -> Optional[int]:
    """
    Create or update actor in database

    Args:
        tmdb_id: TMDB actor ID
        name: Actor name
        country: Country (optional)
        popularity: Popularity score (optional)
        profile_path: Profile image path (optional)

    Returns:
        int: Actor ID if successful, None otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            INSERT INTO {SCHEMA}.actors (tmdb_id, name, country, popularity, profile_path)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (tmdb_id) DO UPDATE SET
                name = EXCLUDED.name,
                country = EXCLUDED.country,
                popularity = EXCLUDED.popularity,
                profile_path = EXCLUDED.profile_path
            RETURNING id
        """, (tmdb_id, name, country, popularity, profile_path))

        actor_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()

        return actor_id

    except Exception as e:
        print(f"Error creating/updating actor: {str(e)}")
        return None


def get_actor_by_tmdb_id(tmdb_id: int) -> Optional[Dict[str, Any]]:
    """
    Get actor by TMDB ID

    Args:
        tmdb_id: TMDB actor ID

    Returns:
        dict: Actor data or None if not found
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(f"SELECT * FROM {SCHEMA}.actors WHERE tmdb_id = %s", (tmdb_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return None

    except Exception as e:
        print(f"Error getting actor: {str(e)}")
        return None


# ==================== SCRIPT CASTING OPERATIONS ====================

def create_script_casting(script_id: int, actor_id: int, role_name: str,
                         match_score: Optional[float] = None) -> Optional[int]:
    """
    Create a script casting entry

    Args:
        script_id: Script ID
        actor_id: Actor ID
        role_name: Role name
        match_score: Match score (optional)

    Returns:
        int: Casting ID if successful, None otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            INSERT INTO {SCHEMA}.script_casting (script_id, actor_id, role_name, match_score)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (script_id, actor_id, role_name, match_score))

        casting_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()

        return casting_id

    except Exception as e:
        print(f"Error creating script casting: {str(e)}")
        return None


def get_casting_by_script(script_id: int) -> List[Dict[str, Any]]:
    """
    Get all casting entries for a script with actor details

    Args:
        script_id: Script ID

    Returns:
        list: List of casting dictionaries with actor info
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(f"""
            SELECT sc.*, a.name, a.tmdb_id, a.country, a.popularity, a.profile_path
            FROM {SCHEMA}.script_casting sc
            JOIN {SCHEMA}.actors a ON sc.actor_id = a.id
            WHERE sc.script_id = %s
            ORDER BY sc.match_score DESC
        """, (script_id,))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    except Exception as e:
        print(f"Error getting casting: {str(e)}")
        return []


# ==================== REVENUE FORECASTS OPERATIONS ====================

def create_revenue_forecast(script_id: int, genre: str, product_category: str,
                           estimated_revenue: float, estimated_roi: float,
                           market_reach: Optional[str] = None) -> Optional[int]:
    """
    Create a revenue forecast

    Args:
        script_id: Script ID
        genre: Genre
        product_category: Product category
        estimated_revenue: Estimated revenue
        estimated_roi: Estimated ROI
        market_reach: Market reach (optional)

    Returns:
        int: Forecast ID if successful, None otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            INSERT INTO {SCHEMA}.revenue_forecasts
            (script_id, genre, product_category, estimated_revenue, estimated_roi, market_reach)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (script_id, genre, product_category, estimated_revenue, estimated_roi, market_reach))

        forecast_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()

        return forecast_id

    except Exception as e:
        print(f"Error creating revenue forecast: {str(e)}")
        return None


def get_forecasts_by_script(script_id: int) -> List[Dict[str, Any]]:
    """
    Get all revenue forecasts for a script

    Args:
        script_id: Script ID

    Returns:
        list: List of forecast dictionaries
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(f"""
            SELECT * FROM {SCHEMA}.revenue_forecasts
            WHERE script_id = %s
            ORDER BY forecast_date DESC
        """, (script_id,))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    except Exception as e:
        print(f"Error getting forecasts: {str(e)}")
        return []


# ==================== STATISTICS AND ANALYTICS ====================

def get_database_stats() -> Dict[str, int]:
    """
    Get database statistics

    Returns:
        dict: Statistics including counts for each table
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        stats = {}

        for table in ['scripts', 'product_placements', 'actors', 'script_casting', 'revenue_forecasts']:
            cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")
            stats[table] = cursor.fetchone()[0]

        conn.close()

        return stats

    except Exception as e:
        print(f"Error getting database stats: {str(e)}")
        return {}


def get_genre_distribution() -> List[Tuple[str, int]]:
    """
    Get distribution of scripts by genre

    Returns:
        list: List of tuples (genre, count)
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT genre, COUNT(*) as count
            FROM {SCHEMA}.scripts
            GROUP BY genre
            ORDER BY count DESC
        """)

        rows = cursor.fetchall()
        conn.close()

        return [(row[0], row[1]) for row in rows]

    except Exception as e:
        print(f"Error getting genre distribution: {str(e)}")
        return []
