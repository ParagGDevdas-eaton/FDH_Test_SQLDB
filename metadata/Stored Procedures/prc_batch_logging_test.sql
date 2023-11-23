Create procedure [metadata].[prc_batch_logging_test] 
(@paramSpTableName nvarchar(max),
@paramSpLastRunDate varchar(max),
@paramSpCurrentRunDate varchar(max),
@paramSpIsHistLoad int,
@paramSpBatchId bigint,
@paramSpSourceId bigint,
@paramSpJobId bigint,
@paramSpSourceJobId int,
@paramSpTriggerId varchar(250),
@paramSpBatchStartDate datetime = NULL,
@paramSpBatchEndDate datetime = NULL,
@paramSpPipelineName varchar(255),
@paramSpBatchLogMessage varchar(4000),
@paramSpBatchRunStatus varchar(255),
@paramSpTriggerTime datetime = NULL,
@paramSpIsReRun int)
as
set nocount on;
BEGIN TRY
--begin
DECLARE @varCount1 INT
DECLARE @varCount2 INT
declare @batch_status varchar(255)=''
Declare @BatchRunStatus	varchar(255)=''
   
print('sp start')
if (isnull(@paramSpBatchId,'') = '')
begin

--Validation for IsReRun value apart from 0 and 1
if (@paramSpIsReRun not in (1,0))
THROW 50010, 'paramSpIsReRun can only be 1 or 0', 1;

--Validating sourid parameters
if not exists(select top 1 1 from  metadata.job_object_details with(nolock) where source_id =@paramSpSourceId)
	THROW 50010, 'Please pass correct values to parameters paramSpSourceId', 1;

--Validating the tablenames provided in pipeline exits in the job_object_details table	
if isnull(@paramSpTableName,'') <> ''
BEGIN
	SET @varCount1 = (SELECT count(1) FROM string_split(@paramSpTableName,','))
	SET @varCount2 = (select count(distinct table_name)  FROM metadata.job_object_details  jod  with(nolock)
		inner join metadata.object o  with(nolock) on o.object_id=jod.object_id 
		where jod.source_id = @paramSpSourceId and jod.enabled = 1 
		and o.table_name in (SELECT value FROM string_split(@paramSpTableName,',')))

	if @varCount1 <> @varCount2
	begin
		--if count is not matching then returning list of tables which are incorrect
		declare @issuetableslist varchar(max),@errormessage varchar(max)
		select @issuetableslist=STRING_AGG(table_name,',') from 
		(SELECT distinct table_name=value  FROM string_split(@paramSpTableName,',')
		EXCEPT
		select distinct table_name  FROM metadata.job_object_details  jod  with(nolock)
		inner join metadata.object o  with(nolock) on o.object_id=jod.object_id 
		where jod.source_id = 1 and jod.enabled = 1 
		and o.table_name in (SELECT value FROM string_split(@paramSpTableName,',')))x

		set @errormessage='Please pass correct table names , issue table names are: '+@issuetableslist;

		THROW 50010,@errormessage, 1;
	end
end

--Validation for incorrect date passed in the LastRunDate parameter in pipeline
If ISDATE(ISNULL(@paramSpLastRunDate,'9999-12-01 00:00')) = 0 
THROW 50010,' Please enter date in valid format for paramSpLastRunDate',1;

--Validation for incorrect date passed in the CurrentRunDate parameter in pipeline
If ISDATE(ISNULL(@paramSpCurrentRunDate,'9999-12-01 00:00')) = 0
THROW 50010,' Please enter date in valid format for paramSpCurrentRunDate',1;

--either @paramSpEtlStartdt and @paramSpCurrentRundt should not be null
IF (@paramSpTriggerTime is null and @paramSpCurrentRunDate is null)
	THROW 50010, 'TriggerTime or current_rundt should be given', 1;

--Validation for LastRunDate greater than CurrentRunDate
if (@paramSpLastRunDate is not null and @paramSpCurrentRunDate is not null and @paramSpLastRunDate > @paramSpCurrentRunDate)
THROW 50010, 'paramSpLastRunDate should be less than paramSpCurrentRunDate ', 1;       

--validating TriggerTime should be less that currentrundate
IF (@paramSpTriggerTime is not null and @paramSpCurrentRunDate is not null and @paramSpTriggerTime < @paramSpCurrentRunDate)
	THROW 50010, ' current_rundt should be less than TriggerTime ', 1;

--validating TriggerTime should be less that currentrundate
IF (@paramSpTriggerTime is not null and @paramSpLastRunDate is not null and @paramSpTriggerTime < @paramSpLastRunDate)
	THROW 50010, ' paramSpTriggerTime should be less than paramSpLastRundt ', 1;	
	
--Validation for IsHistLoad value apart from 0 and 1
if (@paramSpIsHistLoad not in (1,0))
THROW 50010, 'paramSpIsHistLoad can only be 1 or 0', 1;

	select top 1 @batch_status=batch_run_status,@paramSpBatchId=batch_id from metadata.batch_run_details with(nolock) where source_id=@paramSpSourceId  order by batch_id desc
	if (isnull(@batch_status,'')='Active')
		THROW 50010, 'Previous batch is already running', 1;  		
	else if ((isnull(@batch_status,'')='Failed' or isnull(@batch_status,'')='Partially Failed') AND @paramSpIsReRun=0)
		 begin
			print ('Failed') 
			update metadata.batch_run_details
			set batch_run_status='Active'
			where batch_id=@paramSpBatchId 
		 end
	else
	begin
		if (isnull(@paramSpBatchRunStatus,'')<>'Active')
		THROW 50010, 'paramSpBatchRunStatus should be Active for new run', 1; 
		
		insert into metadata.batch_run_details(trigger_id,batch_start_date,trigger_time,pipeline_name,source_id,job_id,batch_run_status) values(@paramSpTriggerId,@paramSpBatchStartDate,@paramSpTriggerTime,@paramSpPipelineName,@paramSpSourceId,@paramSpJobId,@paramSpBatchRunStatus)
		select @paramSpBatchId = Scope_Identity() 
	end
end
else if (@paramSpBatchRunStatus = 'Successfully Completed')
begin

	--- check if FCCS is completed or not if yes then include recon for count check
	IF ((Select count(1) from metadata.audit_log where job_object_id in (Select job_object_id from metadata.job_object_details 
	  	 where target_object_name='dwh.rpt_general_ledger_fccs')	and  convert(date,log_date)=convert(date,getdate()-1))>0)
	 Begin
	    if (isnull((select count(distinct job_object_id) FROM metadata.[audit_log] with(nolock) where batch_id=@paramSpBatchId and status='success'),0)  =
			   isnull((select count(distinct job_object_id) FROM metadata.job_object_details with(nolock) where source_id=@paramSpSourceId and enabled=1),0))
		 	set @BatchRunStatus= 'Successfully Completed' 		 
		else
			set @BatchRunStatus= 'Partially Failed'

	 end
	 else --- Exclude Job id 6,7 (summarized,recon) while checking success count
	  Begin
		if (isnull((select count(distinct job_object_id) FROM metadata.[audit_log] with(nolock) where batch_id=@paramSpBatchId and status='success'),0)  =
			   isnull((select count(distinct job_object_id) FROM metadata.job_object_details with(nolock) where source_id=@paramSpSourceId and job_id not in (6,7) and enabled=1),0))
			set @BatchRunStatus= 'Successfully Completed' 		 
		else
			set @BatchRunStatus= 'Partially Failed'	 
	  End

	  update metadata.batch_run_details
	  set batch_end_date=@paramSpBatchEndDate,batch_log_message=@paramSpBatchLogMessage,batch_run_status=@BatchRunStatus 
	  where batch_id=@paramSpBatchId    
end
else
begin
-- updating batch status
	update metadata.batch_run_details
		set batch_end_date=@paramSpBatchEndDate,batch_log_message=@paramSpBatchLogMessage,batch_run_status=@paramSpBatchRunStatus
		where batch_id=@paramSpBatchId 

end
--returning values to procedure
select @paramSpBatchId as batch_id  , @paramSpBatchStartDate as batch_start_date , @paramSpBatchEndDate as batch_end_date
END TRY
BEGIN CATCH
 Declare @activity varchar(50) = ERROR_PROCEDURE(),
          @error_code varchar(100) = ERROR_NUMBER(),
		  @error_type varchar(250) = 'TechnicalError',
		  @error_message varchar(250) = ERROR_MESSAGE(),
		  @log_date date = getdate()

         
--updating error log
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
--end