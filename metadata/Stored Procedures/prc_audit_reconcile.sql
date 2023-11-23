
--exec [metadata].[prc_audit_reconcile] 194,'refinedReconcile','success',4,3,1
--exec [metadata].[prc_audit_reconcile] 194,'refinedReconcile','failed',3,1,1

 CREATE  procedure [metadata].[prc_audit_reconcile]
@paramSpBatchId bigint,
@paramSpTopic varchar(255),
@paramSpStatus varchar(50),
@paramSpSourceId int,
@paramSpJobId int

AS
BEGIN TRY

-- this sp is used to reconsile raw vs DQ vs refined
SET NOCOUNT ON;
print('sp start')
--validating batchid should not be null
IF (isnull(@paramSpBatchId,0)=0)
	THROW 50010, 'batch_id cannot be null', 1;

--validating batchid 
IF NOT EXISTS(SELECT TOP 1 1 FROM  metadata.batch_run_details WITH(NOLOCK) WHERE batch_id=@paramSpBatchId)
	THROW 50010, 'Please pass valid batch number', 1;
 
--validating @paramSpSourceId parameter
IF NOT EXISTS(SELECT TOP 1 1 FROM  metadata.source WITH(NOLOCK) WHERE source_id in (@paramSpSourceId))
	THROW 50010, 'Please pass valid Source Id ', 1;
 
declare @body varchar(max)
declare @bodyheader varchar(max)
declare @srcName varchar(max)
declare @tablecount bigint

Select @srcName= case when @paramSpSourceId=1 then 'Oracle'
					  when @paramSpSourceId=2 then 'SAP'
					  when @paramSpSourceId=3 then 'FCCS'
					  when @paramSpSourceId=4 then 'TPH'
				end  

set @tablecount = 0
set @bodyheader='Hi Team, <br/><br/> Please find data count mismatched table list (DI vs DQ vs RF) for Source : '+ @srcName + ' and  Batchid: '+convert(varchar(50),@paramSpBatchId ) + '<br/><br/>'
-------------------------------------
Select @tablecount=count(1)
from
		 (
		select obj.table_name as DI_source_object_name,sum(a.source_row_count) as DI_source_row_count,sum(a.target_row_count) as DI_target_row_count,o.object_id as DI_object_id 
		from metadata.audit_log a inner join metadata.job_object_details o on a.job_object_id=o.job_object_id
		inner join metadata.object obj  on o.object_id=obj.object_id
		where  o.source_id=@paramSpSourceId and o.job_id=1  and batch_id=@paramSpBatchId and o.enabled=1
		group by obj.table_name,o.object_id
		) DI
		left join
		(
		select o.source_object_name as DQ_source_object_name,sum(a.source_row_count) as DQ_source_row_count,sum(a.target_row_count) as DQ_target_row_count,sum(rejected_duplicate_count) as DQ_rejected_duplicate_count,o.object_id as DQ_object_id 
		from metadata.audit_log a inner join metadata.job_object_details o on a.job_object_id=o.job_object_id
		where  o.source_id=@paramSpSourceId and o.job_id=2  and batch_id=@paramSpBatchId and o.enabled=1
		group by o.source_object_name,o.object_id
		) DQ on DI.DI_object_id=DQ.DQ_object_id
		left join
		(
		select o.source_object_name as RF_source_object_name,sum(a.source_row_count) as RF_source_row_count,sum(a.target_row_count) as RF_target_row_count,o.object_id as RF_object_id 
		from metadata.audit_log a inner join metadata.job_object_details o on a.job_object_id=o.job_object_id
		where  o.source_id=@paramSpSourceId and o.job_id=3  and batch_id=@paramSpBatchId and o.enabled=1
		group by o.source_object_name,o.object_id
		)RF on DI.DI_object_id=RF.RF_object_id
		where isnull(DI.DI_target_row_count,0)<>isnull(DQ.DQ_target_row_count,0) or isnull(DQ.DQ_target_row_count,0)<>isnull(RF.RF_target_row_count,0)

------------------------------------
 	set @body = cast( (
 	Select  td = isnull(DI_source_object_name,'') + '</td><td>' + cast( isnull(DI_source_row_count,0) as varchar(30) ) + '</td><td>' + cast( isnull(DI_target_row_count,0) as varchar(30) )+'</td><td>' + cast( isnull(DQ_source_row_count,0) as varchar(30) ) + '</td><td>' + cast( isnull(DQ_target_row_count,0) as varchar(30) )+ '</td><td>' + cast( isnull(DQ_rejected_duplicate_count,0) as varchar(30) )+
	+ '</td><td>' + cast( isnull(RF_source_row_count,0) as varchar(30))  + '</td><td>' + cast( isnull(RF_target_row_count,0) as varchar(30))
	from		 
		(select obj.table_name as DI_source_object_name,sum(a.source_row_count) as DI_source_row_count,sum(a.target_row_count) as DI_target_row_count,o.object_id as DI_object_id 
		from metadata.audit_log a inner join metadata.job_object_details o on a.job_object_id=o.job_object_id
		inner join metadata.object obj  on o.object_id=obj.object_id
		where  o.source_id=@paramSpSourceId and o.job_id=1  and batch_id=@paramSpBatchId and o.enabled=1
		group by obj.table_name,o.object_id
		) DI
		left join
		(
		select o.source_object_name as DQ_source_object_name,sum(isnull(a.source_row_count,0)) as DQ_source_row_count,sum(isnull(a.target_row_count,0)) as DQ_target_row_count,sum(isnull(rejected_duplicate_count,0)) as DQ_rejected_duplicate_count,o.object_id as DQ_object_id 
		from metadata.audit_log a inner join metadata.job_object_details o on a.job_object_id=o.job_object_id
		where  o.source_id=@paramSpSourceId and o.job_id=2  and batch_id=@paramSpBatchId and o.enabled=1
		group by o.source_object_name,o.object_id
		) DQ on DI.DI_object_id=DQ.DQ_object_id
		left join
		(
		select o.source_object_name as RF_source_object_name,sum(isnull(a.source_row_count,0)) as RF_source_row_count,sum(isnull(a.target_row_count,0)) as RF_target_row_count,o.object_id as RF_object_id 
		from metadata.audit_log a inner join metadata.job_object_details o on a.job_object_id=o.job_object_id
		where  o.source_id=@paramSpSourceId and o.job_id=3  and batch_id=@paramSpBatchId and o.enabled=1
		group by o.source_object_name,o.object_id
		)RF on DI.DI_object_id=RF.RF_object_id
		where isnull(DI.DI_target_row_count,0)<>isnull(DQ.DQ_target_row_count,0) or isnull(DQ.DQ_target_row_count,0)<>isnull(RF.RF_target_row_count,0)
	 for xml path( 'tr' ), type ) as varchar(max) )

	set @body = @bodyheader + '<table cellpadding=''2'' cellspacing=''2'' border=''1''>'
			  + '<tr><th>Execution Layer</th><th colspan=''2''>DI </th> <th colspan=''3''>DQ </th> <th colspan=''2''>RF</th></tr>
				 <tr><th> Object Name</th><th>Source count </th><th>Target Count</th> <th>Source Count </th><th>Target Count</th><th>Rejected Count</th> <th> Source Count</th><th>Target Count</th></tr>'
			  + replace( replace( @body, '&lt;', '<' ), '&gt;', '>' )
			  + '</table>'
	
	declare @subject varchar(max),@servername varchar(255)
	select @servername=@@SERVERNAME
	If @tablecount = 0
	 begin 	
	   set @body ='Hi Team, <br/><br/> Record count is matching to all tables in DI VS DQ VS RF for Source : '+ @srcName + ' and  Batchid: '+convert(varchar(50),@paramSpBatchId ) + '<br/><br/>' 
	   set @subject='SourceID: '+convert(varchar(50),@paramSpSourceId)+' DI vs DQ vs RF- count matched, servername: '+@servername+' - ' + convert(varchar(50),GETUTCDATE())
	 end
    else 
		set @subject='SourceID: '+convert(varchar(50),@paramSpSourceId)+ ' DI vs DQ vs RF- count mismatched, servername: '+@servername+' - ' + convert(varchar(50),GETUTCDATE())

set @body=@body +'<br> Thanks, <br> Data Team'
select topic,tolist,@subject as subject,@body as body,@tablecount tablecount from metadata.email_details with(nolock) where topic=@paramSpTopic and status=@paramSpStatus
 
END TRY
BEGIN CATCH
 DECLARE @activity VARCHAR(50) = ERROR_PROCEDURE(),
          @error_code VARCHAR(100) = ERROR_NUMBER(),
		  @error_type VARCHAR(250) = 'TechnicalError',
		  @error_message VARCHAR(250) = ERROR_MESSAGE(),
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
										 NULL,
										 'LogicalError';

THROW 50010,@error_message, 1;
END CATCH;