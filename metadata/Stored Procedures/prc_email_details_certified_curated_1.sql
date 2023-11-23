
--exec [metadata].[prc_email_details_certified_curated] 14,'refinedtocertified-fact','success',4,4,1,'fact'
--exec [metadata].[prc_email_details_certified_curated] 157,'certifiedtocurated','success',1,5,1,'rpt',1
--exec [metadata].[prc_email_details_certified_curated] 157,'certifiedtocurated','failed',1,5,1,'rpt',1
 
 CREATE procedure [metadata].[prc_email_details_certified_curated]
 @paramSpBatchId bigint,@paramSpTopic varchar(255),
 @paramSpStatus varchar(50),
 @paramSpSourceId int,
 @paramSpJobId int,
 @paramSpTableType VARCHAR(MAX),
 @paramSpExecutionSequence int
AS
BEGIN TRY

SET NOCOUNT ON;

--validating batchid should not be null
IF (isnull(@paramSpBatchId,0)=0)
	THROW 50010, 'batch_id cannot be null', 1;

--validating batchid 
IF NOT EXISTS(SELECT TOP 1 1 FROM  metadata.batch_run_details WITH(NOLOCK) WHERE batch_id=@paramSpBatchId)
	THROW 50010, 'Please pass valid batch number', 1;

--validating paramSpIsHistLoad parameter
IF (@paramSpStatus not in ('success','failed'))
	THROW 50010, 'paramSpStatus can only be success or failed', 1;

declare @body varchar(max)
declare @bodyheader varchar(max)
declare @totaltablecount varchar(max) 
declare @totaltimeinmin decimal(11,4) 
declare @totaltimeinhr varchar(max)
set @totaltablecount = 0
set @bodyheader='Hi Team, <br/><br/> Please find '+@paramSpTopic+' execution '+@paramSpStatus+' status for sourceid: '+convert(varchar(50),@paramSpSourceId) + ' batchid: '+convert(varchar(50),@paramSpBatchId ) + ', ExecutionSequence: '+ convert(varchar(50),@paramSpExecutionSequence ) + '<br/><br/>'

if @paramSpStatus='success'
begin
    -------------- Calculating total table count & time taken ---------------------    
	SELECT @totaltablecount=count(1) --over (order by source_object_name) ,
	,@totaltimeinmin=sum(DATEDIFF(SECOND, object_start_date,object_end_date))/60.0  --over (order by source_object_name),0) 		
	FROM metadata.[audit_log] a with(nolock) inner join metadata.job_object_details jod with(nolock) on a.job_object_id=jod.job_object_id 
		and a.source_id=jod.source_id and a.job_id=jod.job_id
	WHERE  batch_id=@paramSpBatchId and a.status=@paramSpStatus and a.source_id=@paramSpSourceId and a.job_id=@paramSpJobId 
	and jod.execution_sequence=@paramSpExecutionSequence
	and jod.target_object_name like '%' +@paramSpTableType+ '%'

	SELECT  @totaltimeinhr=right('0' +CAST( CAST((@totaltimeinmin) AS int) / 60 AS varchar),2) + ':'  + right('0' + CAST(CAST((@totaltimeinmin) AS int) % 60 AS varchar(2)),2)

	set @bodyheader = @bodyheader +'Total table processed = '+@totaltablecount+'<br/> Total execution time  &nbsp;= '+@totaltimeinhr+' (HH:MM)<br/><br/>'
	   --------------------------------------------------------------------------
IF ( @paramSpJobId=4 or  @paramSpJobId=5) --to check if passed values are for facts,dimensions and rpt layer
begin
	set @body = cast( (
	select td = table_name + '</td><td>' + cast( TimeTaken as varchar(30) ) + '</td><td>' 
	+ cast( source_row_count as varchar(30))  + '</td><td>' + cast( target_row_count as varchar(30)) + '</td><td>' + [status]
	from (
		  select obj.table_name, 
			CAST((object_end_date-object_start_date) as time(0))  AS TimeTaken,
			a.[status] ,source_row_count=isnull(a.source_row_count,0),target_row_count=isnull(a.target_row_count,0)
		  from metadata.[audit_log] a with(nolock) inner join metadata.job_object_details o with(nolock) on a.job_object_id=o.job_object_id and a.source_id=o.source_id and a.job_id=o.job_id
			   inner join metadata.object obj on o.object_id=obj.object_id
		  where  batch_id=@paramSpBatchId and a.status=@paramSpStatus and o.source_id=@paramSpSourceId and o.job_id=@paramSpJobId 
				 and o.enabled=1 and o.execution_sequence=@paramSpExecutionSequence and o.target_object_name like '%' +@paramSpTableType+ '%'
		  ) as d
	for xml path( 'tr' ), type ) as varchar(max) )

	set @body = @bodyheader +'<table cellpadding=''2'' cellspacing=''2'' border=''1''>'
			  + '<tr><th>Table Name</th><th>TimeTaken[HH:MM:SS]</th><th>SourceRowCount</th><th>TargetRowCount</th><th>Status</th></tr>'
			  + replace( replace( @body, '&lt;', '<' ), '&gt;', '>' )
			  + '</table>'
	If @totaltablecount = 0
	begin
	    set @body ='Hi Team, <br/><br/> There is no success records for current execution <br/><br/>'	 
	end
end
end
else if @paramSpStatus='failed'
begin
	
	set @body = cast( (
		select td = table_name + '</td><td>' + case when(select count(1) from metadata.error_log e with(nolock) 
		where source_id=@paramSpSourceId and job_id=@paramSpJobId and batch_id=@paramSpBatchId and e.job_object_id=d.job_object_id)>0 then 'Failed' 
		when (select count(1) from metadata.[audit_log] a with(nolock) where source_id=@paramSpSourceId and job_id=@paramSpJobId and batch_id=@paramSpBatchId and a.job_object_id=d.job_object_id)>0 then 'Failed' 
		else 'Skipped' end
		+ '</td><td>' + isnull((select top 1 convert(varchar(25),error_log_id) from metadata.error_log e with(nolock) 
where source_id=@paramSpSourceId and job_id=@paramSpJobId and batch_id=@paramSpBatchId and e.job_object_id=d.job_object_id order by log_date),'')
 		from (
				Select distinct obj.table_name,jod.job_object_id	 
				From metadata.job_object_details jod with(nolock) 
				inner join metadata.object obj on jod.object_id=obj.object_id
				left join metadata.[audit_log] a with(nolock) on a.job_object_id=jod.job_object_id and a. batch_id=@paramSpBatchId
				Where jod.job_id=@paramSpJobId and jod.source_id =@paramSpSourceId and jod.enabled=1 
				       and isnull(a.status,'') not in ('success') and jod.execution_sequence=@paramSpExecutionSequence
					   and jod.target_object_name like '%' +@paramSpTableType+ '%'
			Except
				Select distinct obj.table_name,jod.job_object_id		 
				From metadata.job_object_details jod with(nolock) 
				inner join metadata.object obj on jod.object_id=obj.object_id
				left join metadata.[audit_log] a with(nolock) on a.job_object_id=jod.job_object_id and a. batch_id=@paramSpBatchId
				Where  jod.job_id=@paramSpJobId and jod.source_id =@paramSpSourceId and jod.enabled=1
				and isnull(a.status,'') in ('success') and jod.execution_sequence=@paramSpExecutionSequence
				and jod.target_object_name like '%' +@paramSpTableType+ '%'		
		  ) as d
		 -- order by status desc
	for xml path( 'tr' ), type ) as varchar(max) )
	
	 
	
	set @body = @bodyheader + '<table cellpadding=''2'' cellspacing=''2'' border=''1''>'
			+ '<tr><th>Table Name</th><th>Isfailed</th><th>ErrorLogId</th></tr>'
			+ replace( replace( @body, '&lt;', '<' ), '&gt;', '>' )
			+ '</table>'
	 
	set @body=@body +'<br> Please check error log table for more deatils <br>'
	
 	Select @totaltablecount=LEN(isnull(@body,' '))

	If @totaltablecount = 0
	begin
	    set @body ='Hi Team, <br/><br/> There is no failure records for current execution -- SourceID: '+convert(varchar(50),@paramSpSourceId)+' , BatchID: '+convert(varchar(50),@paramSpBatchId) + ', ExecutionSequence: '+ convert(varchar(50),@paramSpExecutionSequence ) + '<br/><br/>'	 
	end
end

declare @subject varchar(max),@servername varchar(255)

set @body=@body +'<br> Thanks, <br> Data Team'
select @servername=@@SERVERNAME
set @subject='SourceID: '+convert(varchar(50),@paramSpSourceId)+' '+@paramSpTopic+ ' execution '+@paramSpStatus +' status, servername: '+@servername+' - ' + convert(varchar(50),GETUTCDATE())
select topic,tolist,@subject as subject,@body as body, @totaltablecount as totaltablecount from metadata.email_details with(nolock) where topic=@paramSpTopic and status=@paramSpStatus

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