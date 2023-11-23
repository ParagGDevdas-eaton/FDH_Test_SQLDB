



--exec metadata.[prc_getactivetablelist_certified]  1,4,'2023-02-07 05:20:01.686',null,null,365,null,'dim'
--exec metadata.[prc_getactivetablelist_certified]  1,4,'2023-02-07 05:20:01.686',null,null,365,null,'fact'
--exec metadata.[prc_getactivetablelist_certified]  2,4,'2023-02-07 05:20:01.686',null,null,366,null,'dim'
--exec metadata.[prc_getactivetablelist_certified]  2,4,'2023-02-07 05:20:01.686',null,null,366,null,'fact'
CREATE PROC [metadata].[prc_getactivetablelist_certified] (
@paramSpSourceId int,
@paramSpJobId int,
@paramSpEtlStartdt datetime, 
@paramSpLastRundt datetime, 
@paramSpTableName VARCHAR(MAX),
@paramSpBatchId bigint,
@paramSpCurrentRundt datetime,
@paramDimOrFact VARCHAR(MAX))
as
BEGIN TRY

SET NOCOUNT ON;
DECLARE @sql VARCHAR(max)

--validating sourcid,jobid passed to SP
--IF NOT EXISTS(SELECT TOP 1 1 FROM  metadata.job_object_details WITH(NOLOCK) WHERE source_id=@paramSpSourceId and job_id=@paramSpJobId )
--	THROW 50010, 'Please pass correct values to parameters source_id, job_id', 1;

--validating batchid should not be null
IF (isnull(@paramSpBatchId,0)=0)
	THROW 50010, 'batch_id cannot be null', 1;

--validating batchid 
IF NOT EXISTS(SELECT TOP 1 1 FROM  metadata.batch_run_details WITH(NOLOCK) WHERE batch_id=@paramSpBatchId)
	THROW 50010, 'Please pass valid batch number', 1;

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
	
--IF jobid is 1 (i.e sourcetoraw) then below statements are executed
IF (@paramSpJobId=4)
BEGIN  
   -- for SAP check if Oracle dim_xxetn_map_unit_sap_ora is completed or not 
	--IF  @paramSpSourceId=2
	--BEGIN
	declare @lpcnt int=1
	---Waiting till Source dependent tables get completed
	declare @Execution_Sequence int
	set @Execution_Sequence=case when @paramDimOrFact ='dim' then 1 when @paramDimOrFact ='fact' then 2 else 0 end

	while @lpcnt<=20
	begin
		if (select count(distinct dependency_id) from metadata.source_job_dependency where source_id =@paramSpSourceId and job_id=@paramSpJobId and enabled=1 and ExecutionSequence=@Execution_Sequence) !=
		(select count(distinct srcjd.dependency_id) from metadata.source_job_dependency srcjd 
		inner join metadata.audit_log al on srcjd.dependent_job_object_id=al.job_object_id 
		where srcjd.source_id =@paramSpSourceId and srcjd.enabled=1 and srcjd.job_id=@paramSpJobId and srcjd.ExecutionSequence=@Execution_Sequence
		and al.batch_id in 
		(select batch_id from (select source_id,max(batch_id) as batch_id from metadata.batch_run_details where convert(date,batch_start_date)= CONVERT(DATE,GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') group by source_id)x))

		WAITFOR DELAY '00:05:00'
	else
		break;

	print @lpcnt
	set @lpcnt=@lpcnt+1

--	end
end 
  print 'in'
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
		and jod.target_object_name like '%' +@paramDimOrFact+ '%'
        and (--- If table list is provided
				(isnull(@paramSpTableName,'') <> '' and o.table_name in (SELECT value FROM string_split(@paramSpTableName,',')))
			OR
			---- If table list is not provided
			(isnull(@paramSpTableName,'') = '' ))
        and -- Checking execution completed or not for that day/batch_id
			jod.job_object_id not in 
            (SELECT jod.job_object_id FROM metadata.job_object_details  jod with(nolock)
             left join metadata.audit_log a with(nolock) on jod.job_object_id = a.job_object_id and a.batch_id=@paramSpBatchId 
             WHERE jod.job_id=@paramSpJobId and jod.source_id =@paramSpSourceId and jod.enabled=1 and isnull(a.status,'') in ('success')
            )
		and ---Checking dependency table completion
            ((SELECT count(value) FROM string_split((jod.dependent_table_list),',')) = (SELECT count(distinct jod1.job_object_id) FROM metadata.job_object_details jod1  WITH(NOLOCK) 
            inner join metadata.audit_log a  WITH(NOLOCK) on jod1.job_object_id = a.job_object_id and a.batch_id=@paramSpBatchId --in (select batch_id from (select source_id,max(batch_id) as batch_id from metadata.batch_run_details where convert(date,batch_start_date)=convert(date,getdate()) group by source_id)x)
            WHERE jod1.job_id in (3,4) and jod1.source_id =@paramSpSourceId
            and jod1.enabled=1 and isnull(a.status,'') in ('success')
            and jod1.source_object_name in (SELECT value FROM string_split((jod.dependent_table_list),',')))
            )
	   --Checking source dependencies completion
		and jod.job_object_id not in (select distinct srcjd.job_object_id
			from metadata.source_job_dependency srcjd 
			left join metadata.audit_log al on srcjd.dependent_job_object_id=al.job_object_id 
			and al.batch_id in (select batch_id from (select source_id,max(batch_id) as batch_id from metadata.batch_run_details where convert(date,batch_start_date)=CONVERT(DATE,GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time') group by source_id)x)
			where srcjd.source_id =@paramSpSourceId and srcjd.enabled=1 and srcjd.ExecutionSequence=@Execution_Sequence
			and al.job_object_id is null)
  
END
END TRY
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