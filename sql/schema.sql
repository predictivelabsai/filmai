-- Film AI Database Schema (PostgreSQL)

-- Create schema
CREATE SCHEMA IF NOT EXISTS filmai;

-- Scripts table
CREATE TABLE IF NOT EXISTS filmai.scripts (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    genre TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product placements table
CREATE TABLE IF NOT EXISTS filmai.product_placements (
    id SERIAL PRIMARY KEY,
    script_id INTEGER REFERENCES filmai.scripts(id),
    product_name TEXT NOT NULL,
    brand TEXT NOT NULL,
    placement_type TEXT,
    scene_description TEXT,
    estimated_cost DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Actors table
CREATE TABLE IF NOT EXISTS filmai.actors (
    id SERIAL PRIMARY KEY,
    tmdb_id INTEGER UNIQUE,
    name TEXT NOT NULL,
    country TEXT,
    popularity DOUBLE PRECISION,
    profile_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Script casting table
CREATE TABLE IF NOT EXISTS filmai.script_casting (
    id SERIAL PRIMARY KEY,
    script_id INTEGER REFERENCES filmai.scripts(id),
    actor_id INTEGER REFERENCES filmai.actors(id),
    role_name TEXT,
    match_score DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Revenue forecasts table
CREATE TABLE IF NOT EXISTS filmai.revenue_forecasts (
    id SERIAL PRIMARY KEY,
    script_id INTEGER REFERENCES filmai.scripts(id),
    genre TEXT NOT NULL,
    product_category TEXT,
    estimated_revenue DOUBLE PRECISION,
    estimated_roi DOUBLE PRECISION,
    market_reach TEXT,
    forecast_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_scripts_genre ON filmai.scripts(genre);
CREATE INDEX IF NOT EXISTS idx_product_placements_script ON filmai.product_placements(script_id);
CREATE INDEX IF NOT EXISTS idx_actors_tmdb ON filmai.actors(tmdb_id);
CREATE INDEX IF NOT EXISTS idx_script_casting_script ON filmai.script_casting(script_id);
CREATE INDEX IF NOT EXISTS idx_revenue_forecasts_script ON filmai.revenue_forecasts(script_id);
