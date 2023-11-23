
CREATE procedure [metadata].[prc_update_last_run_date]
(
@paramSpBatchStartDate datetime,
@paramSpJobObjectId int
)
as

set nocount on;
BEGIN TRY
--to update last_run_time after data is inserted
update metadata.job_object_details
set last_run_time=@paramSpBatchStartDate where metadata.job_object_details.job_object_id= @paramSpJobObjectId 

END TRY
--end
BEGIN CATCH
 Declare @activity varchar(50) = ERROR_PROCEDURE(),
          @error_code varchar(100) = ERROR_NUMBER(),
		  @error_type varchar(250) = 'TechnicalError',
		  @error_message varchar(250) = ERROR_MESSAGE(),
		  @log_date date = getdate()

         

  EXECUTE [metadata].[prc_update_error_log] NULL,
									     NULL,
                                         NULL,
										 @paramSpJobObjectId,
										 @activity,
										 @error_code,
										 @error_type,
										 @error_message,
										 @log_date,
										 NULL,
										 NULL,
										 NULL,
										 'LogicalError';

THROW 50010,@error_message, 1;

END CATCH;