
CREATE PROCEDURE [metadata].[prc_update_audit_log]
(
@paramSpTriggerId varchar(250) =NULL,
@paramSpBatchId bigint,
@paramSpSourceId int =NULL,
@paramSpJobId int =NULL,
@paramSpJobObjectId int =NULL,
@paramSpSourceRowCount int =NULL,
@paramSpTargetRowCount int =NULL,
@paramSpRejetedRowCount int =NULL,
@paramSpRejectedDuplicateCount int =NULL,
@paramSpTargetSize bigint =NULL,
@paramSpRejectedFilePath varchar(500)=NULL,
@paramSpInputfileRowCount int =NULL,
@paramSpInsertRowCount int =NULL,
@paramSpUpdateRowCount int =NULL,
@paramSpLoadType varchar(100)=NULL,
@paramSpLastRunDate datetime =NULL,
@paramSpLogDate datetime =NULL,
@paramSpObjectStartDate datetime =NULL,
@paramSpObjectEndDate datetime =NULL,
@paramSpQueryUsed varchar(max) =NULL,
@paramSpStatus varchar(50)=NULL,
@paramSpCurrentRunDate datetime =NULL,
@paramSpRejectedCommonlogPath varchar(500) =NULL
)
AS
set nocount on;
BEGIN TRY

INSERT INTO [metadata].[audit_log](trigger_id,batch_id,source_id,job_id,job_object_id,source_row_count,target_row_count,rejeted_row_count,
rejected_duplicate_count,target_size,rejected_file_path,inputfile_row_count,insert_row_count,update_row_count,load_type,last_run_date,log_date,object_start_date,
object_end_date,query,[status],current_run_date,rejected_commonlog_path)
VALUES(
@paramSpTriggerId,
@paramSpBatchId,
@paramSpSourceId,
@paramSpJobId,
@paramSpJobObjectId,
@paramSpSourceRowCount,
@paramSpTargetRowCount,
@paramSpRejetedRowCount,
@paramSpRejectedDuplicateCount,
@paramSpTargetSize,
@paramSpRejectedFilePath,
@paramSpInputfileRowCount,
@paramSpInsertRowCount,
@paramSpUpdateRowCount,
@paramSpLoadType,
@paramSpLastRunDate,
@paramSpLogDate,
@paramSpObjectStartDate,
@paramSpObjectEndDate,
@paramSpQueryUsed,
@paramSpStatus,
@paramSpCurrentRunDate,
@paramSpRejectedCommonlogPath
)


END TRY
--end
BEGIN CATCH
 Declare @activity varchar(50) = ERROR_PROCEDURE(),
          @error_code varchar(100) = ERROR_NUMBER(),
		  @error_type varchar(250) = 'TechnicalError',
		  @error_message varchar(250) = ERROR_MESSAGE(),
		  @log_date_error date = getdate()

         

  EXECUTE [metadata].[prc_update_error_log] NULL,
									     NULL,
                                         NULL,
										 @paramSpJobObjectId,
										 @activity,
										 @error_code,
										 @error_type,
										 @error_message,
										 @log_date_error,
										 NULL,
										 NULL,
										 NULL,
										 'LogicalError';

THROW 50010,@error_message, 1;

END CATCH;