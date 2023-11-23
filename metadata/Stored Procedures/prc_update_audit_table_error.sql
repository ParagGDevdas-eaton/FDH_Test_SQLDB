
CREATE procedure [metadata].[prc_update_audit_table_error]
(
@paramSpStatus varchar(50) ,
@paramSpJobObjectId int,
@paramSpSourceId int,
@paramSpJobId int,
@paramSpBatchId bigint
)
as

set nocount on;
BEGIN TRY

update metadata.audit_log
set status=@paramSpStatus where metadata.audit_log.source_id= @paramSpSourceId and 
metadata.audit_log.job_id= @paramSpJobId and metadata.audit_log.job_object_id= @paramSpJobObjectId and metadata.audit_log.batch_id=@paramSpBatchId;
 
select 1;

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