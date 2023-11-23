CREATE PROCEDURE [metadata].[prc_update_execution_log]
(
@paramSpTriggerId varchar(500) =NULL,
@paramSpBatchId bigint = NULL,
@paramSpJobId int = NULL,
@paramSpJobObjectId int =NULL,
@paramSpStepName varchar(500) =NULL,
@paramSpStartTime datetime =NULL,
@paramSpEndTime datetime = NULL,
@paramSpRowCount int =NULL
)
AS
BEGIN TRY
SET NOCOUNT ON;

INSERT INTO [metadata].[execution_log_details]
VALUES(
@paramSpTriggerId,
@paramSpBatchId,
@paramSpJobId,
@paramSpJobObjectId,
@paramSpStepName,
@paramSpStartTime,
@paramSpEndTime,
@paramSpRowCount
)

END TRY
BEGIN CATCH
 DECLARE @activity VARCHAR(50) = ERROR_PROCEDURE(),
          @error_code VARCHAR(100) = ERROR_NUMBER(),
		  @error_type VARCHAR(250) = 'TechnicalError',
		  @error_message VARCHAR(250) = ERROR_MESSAGE(),
		  @log_date date = getdate()         

  EXECUTE [metadata].[prc_update_error_log] NULL,
									     NULL,
                                         @paramSpJobId,
										 NULL,
										 @activity,
										 @error_code,
										 @error_type,
										 @error_message,
										 @log_date,
										 NULL,
										 NULL,
										 @paramSpBatchId,
										 'LogicalError';

THROW 50010,@error_message, 1;
END CATCH;