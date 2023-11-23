
--exec [metadata].[prc_getactivedatasetlist_powerbi_test_0609] 999,'',2
--exec [metadata].[prc_getactivedatasetlist_powerbi_test_0609] 220,'c7475cd4-04a3-4300-b2b7-1f66eb18642f'

CREATE PROC [metadata].[prc_getactivedatasetlist_powerbi] 
(@paramSpBatchId bigint,
@paramSpDataSetId VARCHAR(max),
@paramExecutionSequence int
)
AS 

BEGIN TRY
SET NOCOUNT ON;
DECLARE @varCount1 INT
DECLARE @varCount2 INT
DECLARE @dayNumber int , @IsWeekEnd int
declare @issuedatasetlist varchar(max),@errormessage varchar(max)

--** Check for weekend & FCCS files are processed and set the  value for Full Refresh and Incremental **
	SET @dayNumber = DATEPART(DW, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time');
	IF(@dayNumber = 7 ) --Saturday = 7	
		set @IsWeekEnd=1		 
	ELSE		
		set @IsWeekEnd=0 
IF @IsWeekEnd=1
	begin
		IF(isnull(@paramSpDataSetId,'')<>'')
			begin


					SET @varCount1 = (SELECT count(1) FROM string_split(@paramSpDataSetId,','))
					SET @varCount2 = (select count(distinct datasetid)  FROM metadata.[powerbi_dataset_details] pbids with(nolock)
						
						where pbids.datasetid in (SELECT value FROM string_split(@paramSpDataSetId,',')))

					if @varCount1 <> @varCount2
					begin
						--if count is not matching then returning list of tables which are incorrect
						--declare @issuedatasetlist varchar(max),@errormessage varchar(max)
						select @issuedatasetlist=STRING_AGG(datasetid,',') from 
						(SELECT distinct datasetid=value FROM string_split(@paramSpDataSetId,',')
						EXCEPT
						select distinct datasetid  FROM metadata.[powerbi_dataset_details]  pbids  with(nolock)
						where pbids.datasetid in (SELECT value FROM string_split(@paramSpDataSetId,',')))x

						set @errormessage='Please pass correct dataset ids , issue dataset ids are: '+@issuedatasetlist;

						THROW 50010,@errormessage, 1;

					end

					else
						begin
							(Select distinct  datasetid
							From [metadata].[powerbi_dataset_details] 
							Where enabled=1 and id not in (SELECT jod.id FROM metadata.[powerbi_dataset_details] jod with(nolock)
							left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
							WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
								and execution_sequence=@paramExecutionSequence
								--and domain_name not in ('General Ledger','General Ledger Tax Report','General Ledger SL','General Ledger Reconciliation Report')
								and (isnull(@paramSpDataSetId,'')<>'' and datasetid in (select value from string_split(@paramSpDataSetId,','))
								
								))
				
						end
			END
		ELSE
			BEGIN
				Select distinct  datasetid
				From [metadata].[powerbi_dataset_details] 
				Where enabled=1 and id not in (SELECT jod.id FROM metadata.[powerbi_dataset_details] jod with(nolock)
				left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
				WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
				and execution_sequence=@paramExecutionSequence
					--and domain_name not in ('General Ledger','General Ledger Tax Report','General Ledger SL','General Ledger Reconciliation Report')
			END
	end
else
	begin
		IF(isnull(@paramSpDataSetId,'')<>'')
			begin


					SET @varCount1 = (SELECT count(1) FROM string_split(@paramSpDataSetId,','))
					SET @varCount2 = (select count(distinct datasetid)  FROM metadata.[powerbi_dataset_details] pbids with(nolock)
						
						where pbids.datasetid in (SELECT value FROM string_split(@paramSpDataSetId,',')))

					if @varCount1 <> @varCount2
					begin
						--if count is not matching then returning list of tables which are incorrect
						--declare @issuedatasetlist varchar(max),@errormessage varchar(max)
						select @issuedatasetlist=STRING_AGG(datasetid,',') from 
						(SELECT distinct datasetid=value FROM string_split(@paramSpDataSetId,',')
						EXCEPT
						select distinct datasetid  FROM metadata.[powerbi_dataset_details]  pbids  with(nolock)
						where pbids.datasetid in (SELECT value FROM string_split(@paramSpDataSetId,',')))x

						set @errormessage='Please pass correct dataset ids , issue dataset ids are: '+@issuedatasetlist;

						THROW 50010,@errormessage, 1;

					end

					else
						begin
							(Select distinct  datasetid
							From [metadata].[powerbi_dataset_details] 
							Where enabled=1 and id not in (SELECT jod.id FROM metadata.[powerbi_dataset_details] jod with(nolock)
							left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
							WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
								and execution_sequence=@paramExecutionSequence and job_frequency!='W'
								--and domain_name not in ('General Ledger','General Ledger Tax Report','General Ledger SL','General Ledger Reconciliation Report')
								and (isnull(@paramSpDataSetId,'')<>'' and datasetid in (select value from string_split(@paramSpDataSetId,','))
								
								))
				
						end
			END
		ELSE
			BEGIN
				Select distinct  datasetid
				From [metadata].[powerbi_dataset_details] 
				Where enabled=1 and id not in (SELECT jod.id FROM metadata.[powerbi_dataset_details] jod with(nolock)
				left join metadata.audit_log a with(nolock) on jod.domain_id = a.job_object_id and a.batch_id =@paramSpBatchId			
				WHERE jod.enabled=1 and isnull(a.status,'') in ('success')) 
				and execution_sequence=@paramExecutionSequence and job_frequency!='W'
					--and domain_name not in ('General Ledger','General Ledger Tax Report','General Ledger SL','General Ledger Reconciliation Report')
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