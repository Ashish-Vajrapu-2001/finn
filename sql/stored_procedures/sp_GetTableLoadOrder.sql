CREATE OR ALTER PROCEDURE control.sp_GetTableLoadOrder
    @SourceSystemId NVARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Recursive CTE for Topological Sort
    WITH DependencyGraph AS (
        -- Base Case: Tables with NO dependencies (Level 0)
        SELECT
            tm.table_id,
            tm.source_system_id,
            ss.source_system_name,
            tm.schema_name,
            tm.table_name,
            tm.primary_key_columns,
            CAST(tm.initial_load_completed AS BIT) AS initial_load_completed, -- Important cast for ADF
            tm.load_priority,
            0 AS dependency_level
        FROM control.table_metadata tm
        INNER JOIN control.source_systems ss ON tm.source_system_id = ss.source_system_id
        LEFT JOIN control.load_dependencies ld ON tm.table_id = ld.table_id
        WHERE tm.is_active = 1
          AND ss.is_active = 1
          AND (@SourceSystemId IS NULL OR tm.source_system_id = @SourceSystemId)
          AND ld.depends_on_table_id IS NULL -- No parents

        UNION ALL

        -- Recursive Step: Tables that depend on tables in previous level
        SELECT
            tm.table_id,
            tm.source_system_id,
            ss.source_system_name,
            tm.schema_name,
            tm.table_name,
            tm.primary_key_columns,
            CAST(tm.initial_load_completed AS BIT) AS initial_load_completed,
            tm.load_priority,
            d.dependency_level + 1
        FROM control.table_metadata tm
        INNER JOIN control.source_systems ss ON tm.source_system_id = ss.source_system_id
        INNER JOIN control.load_dependencies ld ON tm.table_id = ld.table_id
        INNER JOIN DependencyGraph d ON ld.depends_on_table_id = d.table_id
        WHERE tm.is_active = 1
          AND ss.is_active = 1
          AND (@SourceSystemId IS NULL OR tm.source_system_id = @SourceSystemId)
    )
    SELECT DISTINCT
        table_id,
        source_system_id,
        source_system_name,
        schema_name,
        table_name,
        primary_key_columns,
        initial_load_completed,
        dependency_level,
        load_priority
    FROM DependencyGraph
    ORDER BY
        dependency_level ASC, -- Load parents first
        load_priority ASC,    -- Then by explicit priority
        source_system_id,
        table_name;
END
GO
