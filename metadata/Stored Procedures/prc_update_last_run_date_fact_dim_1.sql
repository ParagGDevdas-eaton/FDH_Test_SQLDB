--exec [metadata].[prc_update_last_run_date_fact_dim] '2023-03-02 10:03:34.457',2318,3658569,3716881,'fdda7ec8-6b6d-4b31-a50a-b701967e1a89',2,5,152

CREATE procedure [metadata].[prc_update_last_run_date_fact_dim]
(
@paramSpBatchStartDate datetime,
@paramSpJobObjectId int,
@parmSpSourceRowCount varchar(100),
@parmSpTargetRowCount varchar(100),
@paramSpTriggerId varchar(250) =NULL,
@paramSpSourceId int =NULL,
@paramSpJobId int = NULL,
@paramSpBatchId bigint =NULL
)
as

set nocount on;
BEGIN TRY

Declare @log_date date = getdate()

IF (@parmSpSourceRowCount = @parmSpTargetRowCount )
	update metadata.job_object_details
	set last_run_time=@paramSpBatchStartDate where metadata.job_object_details.job_object_id= @paramSpJobObjectId 
ELSE 
THROW 50010,'Source and target count mismatch', 1;

END TRY
--end
BEGIN CATCH
 Declare @activity varchar(50) = ERROR_PROCEDURE(),
          @error_code varchar(100) = ERROR_NUMBER(),
		  @error_type varchar(250) = 'TechnicalError',
		  @error_message varchar(250) = ERROR_MESSAGE()


  EXECUTE [metadata].[prc_update_error_log] @paramSpTriggerId,
									     @paramSpSourceId,
                                         @paramSpJobId,
										 @paramSpJobObjectId,
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