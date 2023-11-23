
--exec [metadata].[prc_getactivetablelist] 4,1,1,'2023-02-21 00:00:00.000',NULL,NULL,115,NULL,0
 
CREATE PROC [metadata].[prc_prod_data_load_getactivetablelist_refined] (
@paramSpSourceId int,
@paramSpJobId int,
@paramSpEtlStartdt datetime, 
@paramSpLastRundt datetime, 
@paramSpTableName VARCHAR(MAX),
@paramSpBatchId bigint,
@paramSpCurrentRundt datetime,
@paramSpIsHistLoad int)
as
BEGIN TRY

SET NOCOUNT ON;

--validating sourcid,jobid passed to SP
IF NOT EXISTS(SELECT TOP 1 1 FROM  metadata.job_object_details WITH(NOLOCK) WHERE source_id=@paramSpSourceId and job_id=@paramSpJobId)
	THROW 50010, 'Please pass correct values to parameters source_id, job_id', 1;

--validating batchid should not be null
IF (isnull(@paramSpBatchId,0)=0)
	THROW 50010, 'batch_id cannot be null', 1;

--validating batchid 
IF NOT EXISTS(SELECT TOP 1 1 FROM  metadata.batch_run_details WITH(NOLOCK) WHERE batch_id=@paramSpBatchId)
	THROW 50010, 'Please pass valid batch number', 1;

--validating paramSpIsHistLoad parameter
IF (@paramSpIsHistLoad not in (1,0))
	THROW 50010, 'is_hist_load can only be 1 or 0', 1;

--Validation for incorrect date passed in the LastRunDate parameter in pipeline
If ISDATE(ISNULL(@paramSpLastRundt,'9999-12-01 00:00')) = 0 
THROW 50010,' Please enter date in valid format for paramSpLastRunDate',1;

--Validation for incorrect date passed in the CurrentRunDate parameter in pipeline
If ISDATE(ISNULL(@paramSpCurrentRundt,'9999-12-01 00:00')) = 0
THROW 50010,' Please enter date in valid format for paramSpCurrentRunDate',1;

--either @paramSpEtlStartdt and @paramSpCurrentRundt should not be null
IF (@paramSpEtlStartdt is null and @paramSpCurrentRundt is null)
	THROW 50010, 'etl_startdt or current_rundt should be given', 1;

--validating lastrundate should be less that currentrundate
IF (@paramSpLastRundt is not null and @paramSpCurrentRundt is not null and @paramSpLastRundt > @paramSpCurrentRundt)
	THROW 50010, ' last_rundt should be less than current_rundt ', 1;	

--validating currentrundate should be less that EtlStartdt
IF (@paramSpEtlStartdt is not null and @paramSpCurrentRundt is not null and @paramSpEtlStartdt < @paramSpCurrentRundt)
	THROW 50010, ' current_rundt should be less than EtlStartdt ', 1;	

--validating lastrundate should be less that EtlStartdt
IF (@paramSpEtlStartdt is not null and @paramSpLastRundt is not null and @paramSpEtlStartdt < @paramSpLastRundt)
	THROW 50010, ' paramSpEtlStartdt should be less than paramSpLastRundt ', 1;	
	
IF(@paramSpJobId=3)
BEGIN

	SELECT 
	jod.* ,o.table_name,s.source_name,
	SUBSTRING(jod.target_object_name,  1, CHARINDEX('.', jod.target_object_name)-1) target_schema_name,
	SUBSTRING(jod.target_object_name, CHARINDEX('.', jod.target_object_name)+1, LEN(jod.target_object_name)) target_table_name,
 	FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyy-MM-dd HH:mm:ss.fff') as valid_last_run_time,
    FORMAT(GETUTCDATE(),'yyyy-MM-dd HH:mm:ss.fff') as valid_current_run_time
	FROM metadata.job_object_details  jod  WITH(NOLOCK) 
	inner join metadata.source s  WITH(NOLOCK) on jod.source_id = s.source_id
	inner join metadata.job j  WITH(NOLOCK) on jod.job_id = j.job_id
	inner join metadata.object o  WITH(NOLOCK) on o.object_id=jod.object_id

	WHERE j.job_id=@paramSpJobId and s.source_id =@paramSpSourceId 
	    and jod.enabled=1 and s.enabled=1 and j.enabled=1 
		and (--- If table list is provided
				(isnull(@paramSpTableName,'') <> '' and o.table_name in (SELECT value FROM string_split(@paramSpTableName,',')))
			OR
			---- Fetch active table list who are not successfull 
			(isnull(@paramSpTableName,'') = '' 
				and jod.job_object_id not in 
				(SELECT jod.job_object_id FROM metadata.job_object_details  jod with(nolock)
					left join metadata.audit_log a with(nolock) on jod.job_object_id = a.job_object_id and a.batch_id=@paramSpBatchId 
					WHERE jod.job_id=@paramSpJobId and jod.source_id =@paramSpSourceId  and jod.enabled=1 and isnull(a.status,'') in ('success')
				)		
			))

END
END TRY
--end
BEGIN CATCH
 DECLARE @activity VARCHAR(50) = ERROR_PROCEDURE(),
          @error_code VARCHAR(100) = ERROR_NUMBER(),
		  @error_type VARCHAR(250) = 'TechnicalError',
		  @error_message VARCHAR(250) = ERROR_MESSAGE(),
		  @log_date date = getdate()         

  EXECUTE [metadata].[prc_update_error_log] NULL,
									     @paramSpSourceId,
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