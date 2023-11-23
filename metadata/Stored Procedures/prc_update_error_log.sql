
CREATE PROCEDURE [metadata].[prc_update_error_log]
 (
    @paramSpTriggerId varchar(250) =NULL,
	@paramSpSourceId int =NULL,
	@paramSpJobId int = NULL,
    @paramSpJobObjectId int = NULL,
    @paramSpActivity varchar(250) =NULL,
    @paramSpErrorCode varchar(500) =NULL,
    @paramSpErrorType varchar(500) =NULL,
    @paramSpErrorMessage varchar(max) =NULL,
	@paramSpLogDate datetime2 =NULL,
	@paramSpNotebookName varchar(200)=NULL,
	@paramSpMethodName varchar(200)=NULL,
	@paramSpBatchId bigint =NULL,
	@paramSpErrorSubtype varchar(250) = NULL
 )
AS

BEGIN
    SET NOCOUNT ON;
	
    INSERT INTO [metadata].[error_log](trigger_id,source_iD,job_iD,job_object_id,activity,error_code,error_type,[error_message],log_date,notebook_name,method_name,
	batch_id,error_subtype)
    VALUES(
	       @paramSpTriggerId,
	       @paramSpSourceId,
		   @paramSpJobId,
		   @paramSpJobObjectId,
		   @paramSpActivity,
		   @paramSpErrorCode,
		   @paramSpErrorType,
		   @paramSpErrorMessage,
		   @paramSpLogDate,
		   @paramSpNotebookName,
		   @paramSpMethodName,
		   @paramSpBatchId,
		   @paramSpErrorSubtype
          )
END