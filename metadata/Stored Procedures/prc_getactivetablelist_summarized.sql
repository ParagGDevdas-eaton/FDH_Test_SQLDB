--exec metadata.[prc_getactivetablelist_summarized]  1,6,'2023-04-10 04:03:01.139',null,null,195,null,0
--exec metadata.[prc_getactivetablelist_summarized]  1,6,'2023-04-10 04:03:01.139',null,null,215,null,1
CREATE PROC [metadata].[prc_getactivetablelist_summarized]
(@paramSpSourceId int,
@paramSpJobId int,
@paramSpEtlStartdt datetime, 
@paramSpLastRundt datetime, 
@paramSpSummaryName VARCHAR(MAX),
@paramSpBatchId bigint,
@paramSpCurrentRundt datetime,
@paramSpAdhocRun int)
AS

BEGIN TRY
SET NOCOUNT ON;
DECLARE @sql VARCHAR(max)

print ('sp start')

--either @paramSpEtlStartdt and @paramSpCurrentRundt should not be null
IF (@paramSpEtlStartdt is  null and @paramSpCurrentRundt is  null)
	THROW 50010, 'etl_startdt or current_rundt should be given', 1;

----validating lastrundate should be less that currentrundate
IF (@paramSpLastRundt is not null and @paramSpCurrentRundt is not null and @paramSpLastRundt > @paramSpCurrentRundt)
	THROW 50010, ' last_rundt should be less than current_rundt ', 1;	

----validating currentrundate should be less that EtlStartdt
IF (@paramSpEtlStartdt is not null and @paramSpCurrentRundt is not null and @paramSpEtlStartdt < @paramSpCurrentRundt)
	THROW 50010, ' current_rundt should be less than EtlStartdt ', 1;	

----validating lastrundate should be less that EtlStartdt
IF (@paramSpEtlStartdt is not null and @paramSpLastRundt is not null and @paramSpEtlStartdt < @paramSpLastRundt)
	THROW 50010, ' paramSpEtlStartdt should be less than paramSpLastRundt ', 1;	

	Declare @lpcnt int=1

--IF jobid is 6 (i.e summarized) and fccs rpt is completed 1 day before then only we will allow to run summarized 
IF (@paramSpJobId=6) and @paramSpAdhocRun=0 and ((SELECT count(1) FROM metadata.audit_log WHERE job_object_id in (SELECT job_object_id FROM  metadata.job_object_details 
		WHERE target_object_name='dwh.rpt_general_ledger_fccs')	and  convert(date,log_date)=dateadd(day,-1,CONVERT(DATE,GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time')))>0)  
BEGIN
	
	While @lpcnt<=12
	Begin
		---Waiting till Source dependent tables gets completed
		if (SELECT count(dependency_id) FROM metadata.source_job_dependency WHERE source_id =@paramSpSourceId and job_id=@paramSpJobId and enabled=1 and ExecutionSequence=1) !=
		(SELECT count(distinct srcjd.dependency_id) FROM metadata.source_job_dependency srcjd 
		inner join metadata.audit_log al on srcjd.dependent_job_object_id=al.job_object_id 
		WHERE srcjd.source_id =@paramSpSourceId and srcjd.enabled=1 and srcjd.job_id=@paramSpJobId and srcjd.ExecutionSequence=1
		and al.status='success'
		and al.batch_id in 
		(SELECT batch_id FROM (SELECT source_id,max(batch_id) as batch_id 
		FROM metadata.batch_run_details 
		WHERE convert(date,batch_start_date)=convert(date,CONVERT(DATETIME2, getdate()) AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time')		
		group by source_id)x))
			WAITFOR DELAY '00:05:00'
		Else 
			BREAK;	
		print 'out'
		print @lpcnt
		set @lpcnt=@lpcnt+1

	END		
  
	SELECT 
		jod.* ,s.source_name,
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
			and jod.job_id=@paramSpJobId
			and (--- If table list is provided
					(isnull(@paramSpSummaryName,'') <> '' and o.table_name in (SELECT value FROM string_split(@paramSpSummaryName,',')))
				OR
				---- If table list is not provided
				(isnull(@paramSpSummaryName,'') = '' ))
			and -- Checking execution completed or not for that day/batch_id
				jod.job_object_id not in 
				(SELECT jod.job_object_id FROM metadata.job_object_details  jod with(nolock)
				 left join metadata.audit_log a with(nolock) on jod.job_object_id = a.job_object_id and a.batch_id =@paramSpBatchId
				 WHERE jod.job_id=@paramSpJobId and jod.source_id =@paramSpSourceId and jod.enabled=1 and isnull(a.status,'') in ('success')
				)
			and ---dependency table completion Check
            ((SELECT count(value) FROM string_split((jod.dependent_table_list),',')) = (SELECT count(distinct jod1.job_object_id) FROM metadata.job_object_details jod1  WITH(NOLOCK) 
            inner join metadata.audit_log a  WITH(NOLOCK) on jod1.job_object_id = a.job_object_id and a.batch_id =@paramSpBatchId
            WHERE jod1.job_id in (3,4,5) and jod1.source_id =@paramSpSourceId 
            and jod1.enabled=1 and isnull(a.status,'') in ('success')
            and jod1.target_object_name in (SELECT value FROM string_split((jod.dependent_table_list),',')))
            ) 
			--Checking source dependencies completion
		and jod.job_object_id not in (SELECT distinct srcjd.job_object_id
			FROM metadata.source_job_dependency srcjd 
			left join metadata.audit_log al on srcjd.dependent_job_object_id=al.job_object_id 
			and al.batch_id in (SELECT batch_id FROM (SELECT source_id,max(batch_id) as batch_id FROM metadata.batch_run_details 
			WHERE convert(date,batch_start_date)=convert(date,CONVERT(DATETIME2, getdate()) AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time')			
			group by source_id)x)
			WHERE srcjd.source_id =@paramSpSourceId and srcjd.enabled=1 and srcjd.ExecutionSequence=1
			and al.job_object_id is null) 

END
ELSE if (@paramSpJobId=6 and @paramSpAdhocRun=1)
BEGIN

	While @lpcnt<=12
	Begin
		---Waiting till Source dependent tables gets completed
		if (SELECT count(dependency_id) FROM metadata.source_job_dependency WHERE source_id =@paramSpSourceId and job_id=@paramSpJobId and enabled=1 and ExecutionSequence=1  and dependent_job_id>=5) !=
		(SELECT count(distinct srcjd.dependency_id) FROM metadata.source_job_dependency srcjd 
		inner join metadata.audit_log al on srcjd.dependent_job_object_id=al.job_object_id 
		WHERE srcjd.source_id =@paramSpSourceId and srcjd.enabled=1 and srcjd.job_id=@paramSpJobId and srcjd.ExecutionSequence=1 and srcjd.dependent_job_id>=5
		and al.status='success'
		and al.batch_id in 
		(SELECT batch_id FROM (SELECT source_id,max(batch_id) as batch_id 
		FROM metadata.batch_run_details 
		WHERE convert(date,batch_start_date)=convert(date,CONVERT(DATETIME2, getdate()) AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time')		
		group by source_id)x))
			WAITFOR DELAY '00:05:00'
		Else 
			BREAK;	
		print 'out'
		print @lpcnt
		set @lpcnt=@lpcnt+1

	END	
    SELECT 
		jod.* ,s.source_name,
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
			and jod.job_id=@paramSpJobId
			and (--- If table list is provided
					(isnull(@paramSpSummaryName,'') <> '' and o.table_name in (SELECT value FROM string_split(@paramSpSummaryName,',')))
				OR
				---- If table list is not provided
				(isnull(@paramSpSummaryName,'') = '' ))
			and -- Checking execution completed or not for that day/batch_id
				jod.job_object_id not in 
				(SELECT jod.job_object_id FROM metadata.job_object_details  jod with(nolock)
				 left join metadata.audit_log a with(nolock) on jod.job_object_id = a.job_object_id and a.batch_id =@paramSpBatchId
				 WHERE jod.job_id=@paramSpJobId and jod.source_id =@paramSpSourceId and jod.enabled=1 and isnull(a.status,'') in ('success')
				)
			--Checking source dependencies completion
		and jod.job_object_id not in (SELECT distinct srcjd.job_object_id
			FROM metadata.source_job_dependency srcjd 
			left join metadata.audit_log al on srcjd.dependent_job_object_id=al.job_object_id 
			and al.batch_id in (SELECT batch_id FROM (SELECT source_id,max(batch_id) as batch_id FROM metadata.batch_run_details 
			WHERE convert(date,batch_start_date)=convert(date,CONVERT(DATETIME2, getdate()) AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time')			
			group by source_id)x)
			WHERE srcjd.source_id =@paramSpSourceId and srcjd.enabled=1 and srcjd.ExecutionSequence=1 and srcjd.dependent_job_id>=5
			and al.job_object_id is null) 
END
else

  SELECT 
		jod.* ,s.source_name,
		SUBSTRING(jod.target_object_name,  1, CHARINDEX('.', jod.target_object_name)-1) target_schema_name,
		SUBSTRING(jod.target_object_name, CHARINDEX('.', jod.target_object_name)+1, LEN(jod.target_object_name)) target_table_name,
 		FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyy-MM-dd HH:mm:ss.fff') as valid_last_run_time,
		FORMAT(GETUTCDATE(),'yyyy-MM-dd HH:mm:ss.fff') as valid_current_run_time
		FROM metadata.job_object_details  jod  WITH(NOLOCK) 
		inner join metadata.source s  WITH(NOLOCK) on jod.source_id = s.source_id
		inner join metadata.job j  WITH(NOLOCK) on jod.job_id = j.job_id
		inner join metadata.object o  WITH(NOLOCK) on o.object_id=jod.object_id
		WHERE 1=2

END TRY

BEGIN CATCH
 DECLARE  @activity VARCHAR(50) = ERROR_PROCEDURE(),
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