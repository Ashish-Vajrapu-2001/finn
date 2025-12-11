CREATE OR ALTER PROCEDURE control.sp_GetCDCChanges
    @SchemaName NVARCHAR(50),
    @TableName NVARCHAR(100),
    @TableId NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LastSyncVersion BIGINT;
    DECLARE @CurrentVersion BIGINT;
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @PKColumns NVARCHAR(255);
    DECLARE @JoinCondition NVARCHAR(MAX) = '';

    -- 1. Get Metadata
    SELECT @LastSyncVersion = last_sync_version,
           @PKColumns = primary_key_columns
    FROM control.table_metadata
    WHERE table_id = @TableId;

    -- Handle first run case
    IF @LastSyncVersion IS NULL SET @LastSyncVersion = 0;

    -- 2. Get Current Version from Source
    SET @CurrentVersion = CHANGE_TRACKING_CURRENT_VERSION();

    -- 3. Build Dynamic Join Condition for Composite Keys
    -- Splits "Col1,Col2" into "CT.Col1 = T.Col1 AND CT.Col2 = T.Col2"
    SELECT @JoinCondition = STRING_AGG(
        CAST('CT.' + value + ' = T.' + value AS NVARCHAR(MAX)),
        ' AND '
    )
    FROM STRING_SPLIT(@PKColumns, ',');

    -- 4. Build Dynamic SQL
    -- IMPORTANT: We explicitly select _current_sync_version to pass to Databricks
    -- We select T.* (Base Table) but only join if the operation is NOT Delete
    SET @SQL = N'
    SELECT
        CT.*,
        ' + CAST(@CurrentVersion AS NVARCHAR(20)) + ' as _current_sync_version,
        T.*
    FROM CHANGETABLE(CHANGES ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ', ' + CAST(@LastSyncVersion AS NVARCHAR(20)) + ') AS CT
    LEFT JOIN ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' AS T
        ON ' + @JoinCondition + '
    WHERE CT.SYS_CHANGE_VERSION <= ' + CAST(@CurrentVersion AS NVARCHAR(20));

    PRINT 'Executing CDC Query for Table: ' + @TableName;
    PRINT 'Version Range: ' + CAST(@LastSyncVersion AS NVARCHAR(20)) + ' to ' + CAST(@CurrentVersion AS NVARCHAR(20));

    -- 5. Execute
    BEGIN TRY
        EXEC sp_executesql @SQL;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMsg NVARCHAR(2000) = ERROR_MESSAGE();
        RAISERROR('Error fetching CDC changes: %s', 16, 1, @ErrorMsg);
    END CATCH
END
GO
