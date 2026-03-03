"""
Utilities package for Film AI
"""

from .pdf_script_extractor import (
    PDFScriptExtractor,
    extract_pdf_text,
    extract_pdf_text_simple
)

from .db_util import (
    get_connection,
    init_database,
    create_script,
    get_script,
    get_all_scripts,
    get_scripts_by_genre,
    update_script,
    delete_script,
    create_product_placement,
    get_placements_by_script,
    create_actor,
    get_actor_by_tmdb_id,
    create_script_casting,
    get_casting_by_script,
    create_revenue_forecast,
    get_forecasts_by_script,
    get_database_stats,
    get_genre_distribution
)

__all__ = [
    'PDFScriptExtractor',
    'extract_pdf_text',
    'extract_pdf_text_simple',
    'get_connection',
    'init_database',
    'create_script',
    'get_script',
    'get_all_scripts',
    'get_scripts_by_genre',
    'update_script',
    'delete_script',
    'create_product_placement',
    'get_placements_by_script',
    'create_actor',
    'get_actor_by_tmdb_id',
    'create_script_casting',
    'get_casting_by_script',
    'create_revenue_forecast',
    'get_forecasts_by_script',
    'get_database_stats',
    'get_genre_distribution'
]
