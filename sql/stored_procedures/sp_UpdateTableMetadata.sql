CREATE OR ALTER PROCEDURE control.sp_UpdateTableMetadata
    @TableId NVARCHAR(50),
    @LastLoadStatus NVARCHAR(20), -- 'success', 'failed'
    @LastLoadTimestamp DATETIME2 = NULL,
    @RowsProcessed INT = NULL,
    @InitialLoadCompleted BIT = NULL,
    @LastSyncVersion BIGINT = NULL,
    @RunId NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- 1. Update Metadata
        UPDATE control.table_metadata
        SET
            last_load_status = @LastLoadStatus,
            last_load_timestamp = ISNULL(@LastLoadTimestamp, GETDATE()),
            rows_processed_last_run = ISNULL(@RowsProcessed, rows_processed_last_run),
            initial_load_completed = ISNULL(@InitialLoadCompleted, initial_load_completed),
            last_sync_version = ISNULL(@LastSyncVersion, last_sync_version),
            last_run_id = ISNULL(@RunId, last_run_id),
            updated_at = GETDATE()
        WHERE table_id = @TableId;

        -- 2. Log Execution History
        INSERT INTO control.pipeline_execution_log
        (table_id, run_id, status, rows_processed, end_time)
        VALUES
        (@TableId, ISNULL(@RunId, 'UNKNOWN'), @LastLoadStatus, @RowsProcessed, GETDATE());

        PRINT 'Updated metadata for table_id: ' + @TableId;
    END TRY
    BEGIN CATCH
        PRINT 'Error updating metadata: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END
GO
