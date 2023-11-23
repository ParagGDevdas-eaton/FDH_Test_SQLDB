--exec [metadata].[prc_getactivetablelist_powerbi_test] 262,'c7475cd4-04a3-4300-b2b7-1f66eb18642f',0,1,'2022-10','PL_MASTERPIPELINE_MAIN_PBI_FULL_REFRESH'
--exec [metadata].[prc_getactivetablelist_powerbi_test] 262,'e6771627-8285-49d9-8108-daaa97f6c263',0,2,'','PL_MASTERPIPELINE_MAIN_PBI_FULL_REFRESH'
--exec [metadata].[prc_getactivetablelist_powerbi] 262,'b58100c8-0cd5-49fd-bec5-f927f93d1489',0,3
--exec [metadata].[prc_getactivetablelist_powerbi] 262,'2a620830-b8ca-4aed-89b9-c839d5fff9b0',0,4



CREATE PROC [metadata].[prc_getactivetablelist_powerbi] 
(@paramSpBatchId bigint,
@paramSpDataSetId VARCHAR(max),
@paramSpAdhocRun int,
@paramExecutionSequence int,
@paramSpPartition varchar(max),
@paramSpPipelineName varchar(max)
--@paramQuarter varchar(max)
--,@paramDomainName VARCHAR(max)
)
AS 

BEGIN TRY

SET NOCOUNT ON;
DECLARE @sql nVARCHAR(max)
DECLARE @varCount1 INT
DECLARE @varCount2 INT
DECLARE @dayNumber int , @IsWeekEnd int
declare @issuepartitionlist varchar(max),@errormessage varchar(max)
DECLARE @maxParallelism nvarchar(max) =(select top 1 maxparallelism from [metadata].[powerbi_dataset_details] where datasetid=@paramSpDataSetId)
DECLARE @resquery nVARCHAR(max)
if @maxParallelism!='NA'
set @resquery= '
{"type":"full", "commitMode":"transactional","maxParallelism":'+@maxParallelism+', "retryCount":1,
"objects":['
else
set @resquery='{"type":"full","objects":['

DECLARE @final_query nvarchar(max)
DECLARE @table_list nvarchar(max)
DECLARE @recalc_list nvarchar(max)
DECLARE @recalc_str nvarchar(max)='{"type":"calculate","objects":['
DECLARE @recalc_body nvarchar(max)
--** Check for weekend & FCCS files are processed and set the  value for Full Refresh and Incremental **
	SET @dayNumber = DATEPART(DW, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time');
	IF(@dayNumber = 7 ) --Saturday = 7	
		set @IsWeekEnd=1		 
	ELSE		
		set @IsWeekEnd=0 
IF OBJECT_ID('tempdb..#temp') IS NOT NULL  
 drop table tempdb..#temp
 CREATE TABLE #temp(
	[job_object_id] [int] NULL,
	[datasetid] [varchar](255) NULL,
	[final_body] [varchar](max) NULL,
	[final_body_recalc] [varchar](max) NULL,
	[groupid] [varchar](max) null
)

--Wait till all the table load gets complete
/*Declare @lpcnt int=1
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
	END*/

IF @IsWeekEnd=1 -- Checking Weekend 
 begin	 
		if @paramExecutionSequence=2 or @paramExecutionSequence=3--for GL and GL-SL domain
			begin
			
				declare @counter int =0;
				declare @counterend int=(select count(distinct quarter_name) from metadata.powerbi_dataset_details where datasetid=@paramSpDataSetId and enabled=1)
				IF OBJECT_ID('tempdb..#temp') IS NOT NULL  
				truncate table #temp;
					while @counter<@counterend
						begin
								set @table_list=stuff((Select ','+ case when (isnull(partition_name,'')) ='' then concat('{"table":"',table_name,'"}') 
								else concat('{"table":"',table_name,'"', ' ,"partition":"',partition_name,'"}') end      
								From [metadata].[powerbi_dataset_details] pbids
								Where enabled=1 and datasetid=@paramSpDataSetId 
								and execution_sequence=@paramExecutionSequence and quarter_name=@counter or quarter_name=null
								and -- Check execution completed or not 
								pbids.domain_id not in 
								(SELECT jod.domain_id FROM metadata.[powerbi_dataset_details] jod with(nolock)
								left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
								WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
								for xml path('')),1,1,'')
							
							
								set @recalc_list=stuff((Select ','+ concat('{"table":"',table_name,'"}') 
								  
								From [metadata].[powerbi_dataset_details] pbids
								Where enabled=1 and datasetid=@paramSpDataSetId and recalc_consider='Y'
								and execution_sequence=@paramExecutionSequence
								and -- Check execution completed or not 
								pbids.domain_id not in 
								(SELECT jod.domain_id FROM metadata.[powerbi_dataset_details] jod with(nolock)
								left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
								WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
								group by table_name for xml path('')),1,1,'')
							
								set @recalc_body=concat(@recalc_str,@recalc_list,']}')
								set @final_query=concat(@resquery,@table_list,']}')
								insert into #temp select top 1 domain_id as job_object_id, @paramSpDataSetId as datasetid,@final_query as final_body,@recalc_body as final_body_recalc,groupid
								from metadata.powerbi_dataset_details 
								where datasetid=@paramSpDataSetId and quarter_name=@counter
								set @counter=@counter+1
						end
					select * from #temp;
			end
			
			else 
				begin
						set @table_list=stuff((Select ','+ case when (isnull(partition_name,'')) ='' then concat('{"table":"',table_name,'"}') 
						else concat('{"table":"',table_name,'"', ' ,"partition":"',partition_name,'"}') end      
						From [metadata].[powerbi_dataset_details] pbids
						Where enabled=1 and datasetid=@paramSpDataSetId
						and execution_sequence=@paramExecutionSequence
						and -- Check execution completed or not 
						pbids.domain_id not in 
						(SELECT jod.domain_id FROM metadata.[powerbi_dataset_details] jod with(nolock)
						left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
						WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
						for xml path('')),1,1,'')
					
				
						set @recalc_list=stuff((Select ','+ concat('{"table":"',table_name,'"}') 
						  
						From [metadata].[powerbi_dataset_details] pbids
						Where enabled=1 and datasetid=@paramSpDataSetId and recalc_consider='Y'
						and execution_sequence=@paramExecutionSequence
						and -- Check execution completed or not 
						pbids.domain_id not in 
						(SELECT jod.domain_id FROM metadata.[powerbi_dataset_details] jod with(nolock)
						left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
						WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
						group by table_name for xml path('')),1,1,'')
				
						set @recalc_body=concat(@recalc_str,@recalc_list,']}')
						set @final_query=concat(@resquery,@table_list,']}')
						select top 1 domain_id as job_object_id, @paramSpDataSetId as datasetid,@final_query as final_body,@recalc_body as final_body_recalc,groupid
						from metadata.powerbi_dataset_details
						where datasetid=@paramSpDataSetId
			
				end
end
else if @paramSpAdhocRun=1 and @paramSpPipelineName='PL_MASTERPIPELINE_MAIN_PBI_FULL_REFRESH' --checking if Adhoc
	begin
		if @paramExecutionSequence=2 or @paramExecutionSequence=3--for GL and GL-SL domain
			begin
			
				declare @counter_two int =0;
				declare @countertwo_end int=(select count(distinct quarter_name) from metadata.powerbi_dataset_details where datasetid=@paramSpDataSetId and enabled=1)
				IF OBJECT_ID('tempdb..#temp') IS NOT NULL  
				truncate table #temp;
					while @counter_two<@countertwo_end
						begin
								set @table_list=stuff((Select ','+ case when (isnull(partition_name,'')) ='' then concat('{"table":"',table_name,'"}') 
								else concat('{"table":"',table_name,'"', ' ,"partition":"',partition_name,'"}') end      
								From [metadata].[powerbi_dataset_details] pbids
								Where enabled=1 and datasetid=@paramSpDataSetId 
								and execution_sequence=@paramExecutionSequence and quarter_name=@counter_two or quarter_name=null
								and -- Check execution completed or not 
								pbids.domain_id not in 
								(SELECT jod.domain_id FROM metadata.[powerbi_dataset_details] jod with(nolock)
								left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
								WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
								for xml path('')),1,1,'')
							
							
								set @recalc_list=stuff((Select ','+ concat('{"table":"',table_name,'"}') 
								  
								From [metadata].[powerbi_dataset_details] pbids
								Where enabled=1 and datasetid=@paramSpDataSetId and recalc_consider='Y'
								and execution_sequence=@paramExecutionSequence
								and -- Check execution completed or not 
								pbids.domain_id not in 
								(SELECT jod.domain_id FROM metadata.[powerbi_dataset_details] jod with(nolock)
								left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
								WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
								group by table_name for xml path('')),1,1,'')
							
								set @recalc_body=concat(@recalc_str,@recalc_list,']}')
								set @final_query=concat(@resquery,@table_list,']}')
								insert into #temp select top 1 domain_id as job_object_id, @paramSpDataSetId as datasetid,@final_query as final_body,@recalc_body as final_body_recalc,groupid
								from metadata.powerbi_dataset_details 
								where datasetid=@paramSpDataSetId and quarter_name=@counter_two
								set @counter_two=@counter_two+1
						end
					select * from #temp;
			end
			
			else 
				begin
						set @table_list=stuff((Select ','+ case when (isnull(partition_name,'')) ='' then concat('{"table":"',table_name,'"}') 
						else concat('{"table":"',table_name,'"', ' ,"partition":"',partition_name,'"}') end      
						From [metadata].[powerbi_dataset_details] pbids
						Where enabled=1 and datasetid=@paramSpDataSetId
						and execution_sequence=@paramExecutionSequence
						and -- Check execution completed or not 
						pbids.domain_id not in 
						(SELECT jod.domain_id FROM metadata.[powerbi_dataset_details] jod with(nolock)
						left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
						WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
						for xml path('')),1,1,'')
					
				
						set @recalc_list=stuff((Select ','+ concat('{"table":"',table_name,'"}') 
						  
						From [metadata].[powerbi_dataset_details] pbids
						Where enabled=1 and datasetid=@paramSpDataSetId and recalc_consider='Y'
						and execution_sequence=@paramExecutionSequence
						and -- Check execution completed or not 
						pbids.domain_id not in 
						(SELECT jod.domain_id FROM metadata.[powerbi_dataset_details] jod with(nolock)
						left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
						WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
						group by table_name for xml path('')),1,1,'')
				
						set @recalc_body=concat(@recalc_str,@recalc_list,']}')
						set @final_query=concat(@resquery,@table_list,']}')
						select top 1 domain_id as job_object_id, @paramSpDataSetId as datasetid,@final_query as final_body,@recalc_body as final_body_recalc,groupid
						from metadata.powerbi_dataset_details
						where datasetid=@paramSpDataSetId
			
				end
	end
ELSE
	begin
		
		IF(isnull(@paramSpPartition,'')<>'')
			begin


					SET @varCount1 = (SELECT count(1) FROM string_split(@paramSpPartition,','))
					SET @varCount2 = (select count(distinct datasetid)  FROM metadata.[powerbi_dataset_details] pbids with(nolock)
						
						where  pbids.datasetid=@paramSpDataSetId and pbids.partition_name in (SELECT value FROM string_split(@paramSpPartition,','))) 

					if @varCount1 <> @varCount2
					begin
						--if count is not matching then returning list of tables which are incorrect
						--declare @issuedatasetlist varchar(max),@errormessage varchar(max)
						select @issuepartitionlist=STRING_AGG(partition_name,',') from 
						(SELECT distinct partition_name=value FROM string_split(@paramSpPartition,',')
						EXCEPT
						select distinct partition_name  FROM metadata.[powerbi_dataset_details]  pbids  with(nolock)
						where pbids.datasetid=@paramSpDataSetId and pbids.partition_name in (SELECT value FROM string_split(@paramSpPartition,',')))x

						set @errormessage='Please pass correct dataset ids , issue dataset ids are: '+@issuepartitionlist;

						THROW 50010,@errormessage, 1;

					end

					else
						begin
								set @table_list=stuff((Select ','+ case when (isnull(partition_name,'')) ='' then concat('{"table":"',table_name,'"}') 
								else concat('{"table":"',table_name,'"', ' ,"partition":"',partition_name,'"}') end 
								From [metadata].[powerbi_dataset_details] pbids
								Where pbids.enabled=1 and pbids.datasetid=@paramSpDataSetId and pbids.partition_name=@paramSpPartition
								and execution_sequence=@paramExecutionSequence --and job_frequency='D'
								and -- Check execution completed or not 
								pbids.domain_id not in 
								(
								SELECT jod.domain_id FROM metadata.[powerbi_dataset_details] jod with(nolock)
								left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
								WHERE jod.enabled=1 and isnull(a.status,'') in ('success')
								)
								for xml path('')),1,1,'')
							

								set @recalc_list=stuff((Select ','+ concat('{"table":"',table_name,'"}') 
								  
								From [metadata].[powerbi_dataset_details] pbids
								Where enabled=1 and datasetid=@paramSpDataSetId and recalc_consider='Y'
								and execution_sequence=@paramExecutionSequence --and job_frequency='D'
								and -- Check execution completed or not 
								pbids.domain_id not in 
								(SELECT jod.domain_id FROM metadata.[powerbi_dataset_details] jod with(nolock)
								left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
								WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
								group by table_name for xml path('')),1,1,'')
							
								set @recalc_body=concat(@recalc_str,@recalc_list,']}')
								set @final_query=concat(@resquery,@table_list,']}')
								select top 1 domain_id as job_object_id, @paramSpDataSetId as datasetid,@final_query as final_body,@recalc_body as final_body_recalc,groupid
								from metadata.powerbi_dataset_details
								where datasetid=@paramSpDataSetId
	
				
						end
			END
		ELSE
			BEGIN
					set @table_list=stuff((Select ','+ case when (isnull(partition_name,'')) ='' then concat('{"table":"',table_name,'"}') 
					else concat('{"table":"',table_name,'"', ' ,"partition":"',partition_name,'"}') end 
					From [metadata].[powerbi_dataset_details] pbids
					Where pbids.enabled=1 and pbids.datasetid=@paramSpDataSetId
					and execution_sequence=@paramExecutionSequence --and job_frequency='D'
					and
					(partition_name is null or concat(SUBSTRING(partition_name,1,4),RIGHT(Replicate('0', 2) + SUBSTRING(partition_name,6,LEN(partition_name)),2)) = FORMAT(dateadd(month, -1, GETDATE()), 'yyyyMM') or partition_name is null or concat(SUBSTRING(partition_name,1,4),RIGHT(Replicate('0', 2) + SUBSTRING(partition_name,6,LEN(partition_name)),2))  = FORMAT(dateadd(month, +0, GETDATE()), 'yyyyMM') or partition_name is null or concat(SUBSTRING(partition_name,1,4),RIGHT(Replicate('0', 2) + SUBSTRING(partition_name,6,LEN(partition_name)),2))  = FORMAT(dateadd(month, +1, GETDATE()), 'yyyyMM') or partition_name is null or concat(SUBSTRING(partition_name,1,4),RIGHT(Replicate('0', 2) + SUBSTRING(partition_name,6,LEN(partition_name)),2))  = FORMAT(dateadd(month, +2, GETDATE()), 'yyyyMM'))
					and -- Check execution completed or not 
					pbids.domain_id not in 
					(
					SELECT jod.domain_id FROM metadata.[powerbi_dataset_details] jod with(nolock)
					left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
					WHERE jod.enabled=1 and isnull(a.status,'') in ('success')
					)
					for xml path('')),1,1,'')
				

					set @recalc_list=stuff((Select ','+ concat('{"table":"',table_name,'"}') 
					  
					From [metadata].[powerbi_dataset_details] pbids
					Where enabled=1 and datasetid=@paramSpDataSetId and recalc_consider='Y'
					and execution_sequence=@paramExecutionSequence --and job_frequency='D'
					and -- Check execution completed or not 
					pbids.domain_id not in 
					(SELECT jod.domain_id FROM metadata.[powerbi_dataset_details] jod with(nolock)
					left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
					WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
					group by table_name for xml path('')),1,1,'')
				
					set @recalc_body=concat(@recalc_str,@recalc_list,']}')
					set @final_query=concat(@resquery,@table_list,']}')
					select top 1 domain_id as job_object_id, @paramSpDataSetId as datasetid,@final_query as final_body,@recalc_body as final_body_recalc,groupid
					from metadata.powerbi_dataset_details
					where datasetid=@paramSpDataSetId
	
			END
		
		
		
	
 
	end
END TRY

BEGIN CATCH
 DECLARE @activity VARCHAR(50) = ERROR_PROCEDURE(),
          @error_code VARCHAR(100) = ERROR_NUMBER(),
		  @error_type VARCHAR(250) = 'TechnicalError',
		  @error_message VARCHAR(250) = ERROR_MESSAGE(),
		  @log_date date = getdate()         

  EXECUTE [metadata].[prc_update_error_log] NULL,
									     0,
                                         0,
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