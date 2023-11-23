

CREATE procedure [metadata].[prc_powerbi_batch_logging] 
(@paramSpBatchId bigint,
@paramSpSourceId bigint,
@paramSpJobId bigint,
@paramSpTriggerId varchar(250),
@paramSpBatchStartDate datetime = NULL,
@paramSpBatchEndDate datetime = NULL,
@paramSpPipelineName varchar(255),
@paramSpBatchLogMessage varchar(4000),
@paramSpBatchRunStatus varchar(255),
@paramSpTriggerTime datetime = NULL,
@paramSpIsReRun int,
@paramSpAdhocRun int
)
as
set nocount on;
BEGIN TRY
--begin
DECLARE @varCount1 INT
DECLARE @varCount2 INT
declare @batch_status varchar(255)=''
Declare @BatchRunStatus	varchar(255)=''
DECLARE @dayNumber int , @IsWeekEnd int
	SET @dayNumber = DATEPART(DW, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time');
	IF(@dayNumber = 7 or @dayNumber=1 ) --Saturday = 7	--sunday=1
		set @IsWeekEnd=1		 
	ELSE		
		set @IsWeekEnd=0 


--Wait till all the table load gets complete
if @paramSpAdhocRun=1 and @paramSpPipelineName='PL_MASTERPIPELINE_MAIN_PBI_INCREMENTAL_REFRESH'--if @paramSpAdhocRun=1 we won't check if all ETL batches are complete or not
begin 
WAITFOR DELAY '00:00:00'
end
else
begin
Declare @lpcnt int=1
while @lpcnt<=24
	BEGIN
	
		if (Select (count(batch_id))
		From  metadata.batch_run_details 
		where convert(date,trigger_time)=CONVERT(DATE,GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time')
		and pipeline_name='PL_MASTERPIPELINE_MAIN'
		and batch_run_status <> 'Successfully Completed')>0

			WAITFOR DELAY '00:05:00'
		else
			break;
		print @lpcnt
		set @lpcnt=@lpcnt+1
	END
end

if (isnull(@paramSpBatchId,'') = '')
begin					
 
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

	if(@IsWeekEnd=1)
		begin
			if (isnull((select count(distinct job_object_id) FROM metadata.[audit_log] with(nolock) where batch_id=@paramSpBatchId and status='success'),0)  =
			   isnull((select count(distinct domain_id) FROM metadata.powerbi_dataset_details with(nolock) where enabled=1),0))
			set @BatchRunStatus= 'Successfully Completed' 		 
			else
			set @BatchRunStatus= 'Partially Failed'	 
		end
		else
		begin
			if (isnull((select count(distinct job_object_id) FROM metadata.[audit_log] with(nolock) where batch_id=@paramSpBatchId and status='success'),0)  =
			   isnull((select count(distinct domain_id) FROM metadata.powerbi_dataset_details with(nolock) where enabled=1 and job_frequency='D'),0))
			set @BatchRunStatus= 'Successfully Completed' 		 
			else
			set @BatchRunStatus= 'Partially Failed'	 
		end

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
									     99,
                                         99,
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