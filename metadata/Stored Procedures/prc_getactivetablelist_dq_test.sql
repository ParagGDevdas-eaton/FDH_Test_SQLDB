
--exec [metadata].[prc_getactivetablelist_dq_test] 1,2,'',255
 
CREATE PROC [metadata].[prc_getactivetablelist_dq_test] (
@paramSpSourceId int,
@paramSpJobId int,
@paramSpTableName VARCHAR(MAX),
@paramSpBatchId bigint
)
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
--IF NOT EXISTS(SELECT TOP 1 1 FROM  metadata.batch_run_details WITH(NOLOCK) WHERE batch_id=@paramSpBatchId)
--	THROW 50010, 'Please pass valid batch number', 1;

SELECT jod.* ,s.source_name 
	FROM metadata.job_object_details  jod  WITH(NOLOCK) 
	inner join metadata.source s  WITH(NOLOCK) on jod.source_id = s.source_id
	where jod.source_object_name in ('ap_bank_branches','ap_batches_all','ap_checks_all')
	and jod.job_id=2
--comment starts here--

/*(SELECT jod.* ,s.source_name 
	FROM metadata.job_object_details  jod  WITH(NOLOCK) 
	inner join metadata.source s  WITH(NOLOCK) on jod.source_id = s.source_id 
	inner join metadata.job j  WITH(NOLOCK) on jod.job_id = j.job_id 
	inner join metadata.object o  WITH(NOLOCK) on o.object_id=jod.object_id 
	WHERE j.job_id= @paramSpJobId and s.source_id = @paramSpSourceId
		 and jod.enabled=1 and s.enabled=1 and j.enabled=1 
		and ((isnull(@paramSpTableName,'') <> '' 
        and o.table_name in (SELECT value FROM string_split(@paramSpTableName,','))) 
		OR 
		(isnull(@paramSpTableName,'') = '' 
        and jod.job_object_id  not in 
		(SELECT distinct jod.job_object_id 
        FROM metadata.job_object_details jod WITH(NOLOCK) 
		left join metadata.audit_log a WITH(NOLOCK) on jod.job_object_id = a.job_object_id 
        and a.batch_id= @paramSpBatchId
		WHERE jod.job_id= @paramSpJobId and jod.source_id = @paramSpSourceId 
        and jod.enabled=1 and isnull(a.status,'') in ('success')) 
		
		and (jod.object_id in (SELECT distinct jod.object_id FROM metadata.job_object_details  jod  WITH(NOLOCK) 
		left join metadata.audit_log a  WITH(NOLOCK) on jod.job_object_id = a.job_object_id and a.batch_id= @paramSpBatchId
		WHERE jod.job_id=1 and jod.source_id = @paramSpSourceId and jod.enabled=1 
		and isnull(a.status,'') in ('success')))
		) 
		))*/

--comment ends here--
END TRY

--select * from metadata.job_object_details where job_object_id between 582 and 586
	

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