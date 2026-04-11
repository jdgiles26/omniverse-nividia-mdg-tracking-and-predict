-- Mission Data Grid - PostgreSQL Initialization Script
-- Creates core tables for mission tracking and data grid

-- Enable pgvector extension for embedding vectors
CREATE EXTENSION IF NOT EXISTS vector;

-- Detections table: Raw detections from DeepStream
CREATE TABLE IF NOT EXISTS detections (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    detector_source VARCHAR(255),
    bbox_x FLOAT,
    bbox_y FLOAT,
    bbox_width FLOAT,
    bbox_height FLOAT,
    class_id INTEGER,
    class_label VARCHAR(255),
    confidence FLOAT,
    tracking_id VARCHAR(255),
    frame_number BIGINT,
    metadata JSONB
);

-- Tracking events: Tracked objects across frames
CREATE TABLE IF NOT EXISTS tracking_events (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    tracking_id VARCHAR(255) NOT NULL,
    trajectory_points JSONB,
    velocity_mps FLOAT,
    heading_deg FLOAT,
    classification VARCHAR(255),
    threat_level VARCHAR(50)
);

-- Fused events: Multimodal fusion results
CREATE TABLE IF NOT EXISTS fused_events (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    event_type VARCHAR(255),
    tracking_id VARCHAR(255),
    vss_summary TEXT,
    cosmos_prediction JSONB,
    fusion_confidence FLOAT,
    alert_triggered BOOLEAN DEFAULT FALSE,
    metadata JSONB
);

-- Mission alerts
CREATE TABLE IF NOT EXISTS mission_alerts (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    alert_type VARCHAR(255),
    severity VARCHAR(50),
    message TEXT,
    tracking_id VARCHAR(255),
    location_json JSONB,
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(255),
    acknowledged_at TIMESTAMPTZ
);

-- COSMOS predictions
CREATE TABLE IF NOT EXISTS cosmos_predictions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    tracking_id VARCHAR(255),
    prediction_horizon_seconds INTEGER,
    predicted_trajectory JSONB,
    threat_probability FLOAT,
    confidence FLOAT,
    scenario_type VARCHAR(255)
);

-- VSS summaries
CREATE TABLE IF NOT EXISTS vss_summaries (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    tracking_id VARCHAR(255),
    summary_text TEXT,
    key_frames JSONB,
    entities_identified JSONB,
    activities_identified JSONB
);

-- Data grid snapshots
CREATE TABLE IF NOT EXISTS grid_snapshots (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    snapshot_name VARCHAR(255),
    mission_context JSONB,
    active_tracks INTEGER,
    active_alerts INTEGER,
    data_state JSONB
);

-- Create indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_tracking_events_tracking_id ON tracking_events(tracking_id);
CREATE INDEX IF NOT EXISTS idx_tracking_events_timestamp ON tracking_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fused_events_tracking_id ON fused_events(tracking_id);
CREATE INDEX IF NOT EXISTS idx_mission_alerts_timestamp ON mission_alerts(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_mission_alerts_acknowledged ON mission_alerts(acknowledged);

-- Insert default mission configuration
INSERT INTO grid_snapshots (snapshot_name, mission_context, data_state)
VALUES ('initialization', '{"version": "1.0.0"}', '{"status": "ready"}')
ON CONFLICT DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO mission;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO mission;
