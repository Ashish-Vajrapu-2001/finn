/*
============================================================================
METADATA-DRIVEN CDC PIPELINE
Script: 01_create_control_tables.sql
Description: Creates the schema and tables for the control framework.
============================================================================
*/

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA [control]')
END
GO

-- 1. Source Systems Registry
IF OBJECT_ID('control.source_systems', 'U') IS NOT NULL DROP TABLE control.source_systems;
CREATE TABLE control.source_systems (
    source_system_id NVARCHAR(50) NOT NULL,
    source_system_name NVARCHAR(100) NOT NULL,
    description NVARCHAR(255),
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_source_systems PRIMARY KEY (source_system_id)
);
CREATE INDEX IX_source_systems_active ON control.source_systems(is_active);

-- 2. Table Metadata (The Brain)
IF OBJECT_ID('control.table_metadata', 'U') IS NOT NULL DROP TABLE control.table_metadata;
CREATE TABLE control.table_metadata (
    table_id NVARCHAR(50) NOT NULL,
    source_system_id NVARCHAR(50) NOT NULL,
    source_system_name NVARCHAR(100), -- Denormalized for easier ADF lookup
    schema_name NVARCHAR(50) NOT NULL,
    table_name NVARCHAR(100) NOT NULL,
    primary_key_columns NVARCHAR(255) NOT NULL, -- Comma separated for composite keys
    initial_load_completed BIT DEFAULT 0,
    last_sync_version BIGINT, -- SQL Change Tracking Version
    last_load_status NVARCHAR(20) DEFAULT 'pending', -- 'success', 'failed', 'running'
    last_load_timestamp DATETIME2,
    rows_processed_last_run INT,
    last_run_id NVARCHAR(100), -- ADF Pipeline RunId
    is_active BIT DEFAULT 1,
    load_priority INT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_table_metadata PRIMARY KEY (table_id),
    CONSTRAINT FK_table_metadata_source FOREIGN KEY (source_system_id)
        REFERENCES control.source_systems(source_system_id) ON DELETE CASCADE
);
CREATE INDEX IX_table_metadata_processing ON control.table_metadata(source_system_id, is_active, load_priority);

-- 3. Load Dependencies (Topological Sort Support)
IF OBJECT_ID('control.load_dependencies', 'U') IS NOT NULL DROP TABLE control.load_dependencies;
CREATE TABLE control.load_dependencies (
    dependency_id INT IDENTITY(1,1) NOT NULL,
    table_id NVARCHAR(50) NOT NULL,
    depends_on_table_id NVARCHAR(50) NOT NULL,
    created_at DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_load_dependencies PRIMARY KEY (dependency_id),
    CONSTRAINT FK_load_deps_child FOREIGN KEY (table_id)
        REFERENCES control.table_metadata(table_id) ON DELETE NO ACTION, -- Avoid cycles in CASCADE
    CONSTRAINT FK_load_deps_parent FOREIGN KEY (depends_on_table_id)
        REFERENCES control.table_metadata(table_id) ON DELETE NO ACTION
);
CREATE INDEX IX_load_dependencies_lookup ON control.load_dependencies(table_id);

-- 4. Pipeline Execution Log (Audit Trail)
IF OBJECT_ID('control.pipeline_execution_log', 'U') IS NOT NULL DROP TABLE control.pipeline_execution_log;
CREATE TABLE control.pipeline_execution_log (
    log_id BIGINT IDENTITY(1,1) NOT NULL,
    table_id NVARCHAR(50) NOT NULL,
    run_id NVARCHAR(100) NOT NULL,
    status NVARCHAR(20),
    rows_processed INT,
    start_time DATETIME2 DEFAULT GETDATE(),
    end_time DATETIME2,
    error_message NVARCHAR(MAX),
    created_at DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT PK_pipeline_execution_log PRIMARY KEY (log_id),
    CONSTRAINT FK_execution_log_table FOREIGN KEY (table_id)
        REFERENCES control.table_metadata(table_id) ON DELETE CASCADE
);
CREATE INDEX IX_execution_log_run_id ON control.pipeline_execution_log(run_id);
CREATE INDEX IX_execution_log_history ON control.pipeline_execution_log(table_id, created_at DESC);
GO
