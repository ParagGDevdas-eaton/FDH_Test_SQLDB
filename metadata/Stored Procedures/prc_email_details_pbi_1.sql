
 --exec [metadata].[prc_email_details_pbi] 220,'powerBI','failed',99

 CREATE procedure [metadata].[prc_email_details_pbi]
 @paramSpBatchId bigint,
 @paramSpTopic varchar(255),
 @paramSpStatus varchar(50),
 @paramSpSourceId int,
 @paramSpAdhocRun int

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
DECLARE @dayNumber int , @IsWeekEnd int

--** Check for weekend & FCCS files are processed and set the  value for Full Refresh and Incremental **
	SET @dayNumber = DATEPART(DW, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time');
	IF(@dayNumber = 7 ) --Saturday = 7	
		set @IsWeekEnd=1		 
	ELSE		
		set @IsWeekEnd=0 

set @totaltablecount = 0
set @bodyheader='Hi Team, <br/><br/> Please find '+@paramSpTopic+' execution '+@paramSpStatus+' status for sourceid: '+convert(varchar(50),@paramSpSourceId) + ' batchid: '+convert(varchar(50),@paramSpBatchId ) + '<br/><br/>'



if @paramSpStatus='success'
begin
    -------------- Calculating total table count & time taken ---------------------    
	SELECT @totaltablecount=count(1),@totaltimeinmin=sum(DATEDIFF(SECOND, object_start_date,object_end_date))/60.0
			  FROM metadata.[powerbi_dataset_details] jod with(nolock)
			inner join metadata.audit_log a with(nolock) on jod.id = a.job_object_id and a.batch_id =@paramSpBatchId			
			WHERE jod.enabled=1 and isnull(a.status,'') in ('success')		
		
	SELECT  @totaltimeinhr=right('0' +CAST( CAST((@totaltimeinmin) AS int) / 60 AS varchar),2) + ':'  + right('0' + CAST(CAST((@totaltimeinmin) AS int) % 60 AS varchar(2)),2)

	set @bodyheader = @bodyheader +'Total table processed = '+@totaltablecount+'<br/> Total execution time  &nbsp;= '+@totaltimeinhr+' (HH:MM)<br/><br/>'
	   --------------------------------------------------------------------------
	IF (@paramSpSourceId=99)
	begin
		set @body = cast( (
		select td = cast( id as varchar(30))  + '</td><td>' + cast( domain_name as varchar(100)) + '</td><td>' + cast( table_name as varchar(150)) + '</td><td>'
		+ cast( isnull(partition_name,'') as varchar(50)) + '</td><td>' + cast( TimeTaken as varchar(30))
		from (
			  SELECT jod.id,jod.domain_name,jod.table_name,jod.partition_name,CAST((object_end_date-object_start_date) as time(0))  AS TimeTaken
			  FROM metadata.[powerbi_dataset_details] jod with(nolock)
			inner join metadata.audit_log a with(nolock) on jod.id = a.job_object_id and a.batch_id =@paramSpBatchId			
			WHERE jod.enabled=1 and isnull(a.status,'') in ('success')					   
			  ) as d
		for xml path( 'tr' ), type ) as varchar(max) )
		
		set @body = @bodyheader +'<table cellpadding=''2'' cellspacing=''2'' border=''1''>'
				  + '<tr><th>Id</th><th>Domain Name</th><th>Table Name</th><th>Partition Name</th><th>TimeTaken[HH:MM:SS]</th></tr>'
				  + replace( replace( @body, '&lt;', '<' ), '&gt;', '>' )
				  + '</table>'
		If @totaltablecount = 0
			set @body ='Hi Team, <br/><br/> There is no success records for current execution <br/><br/>'	 
		
	 end
end
else if @paramSpStatus='failed'
Begin

IF (@paramSpSourceId=99)
	begin

		--declare @tablecount varchar(max)
	IF @IsWeekEnd=1 or @paramSpAdhocRun=1 -- Checking Weekend & Adhoc
	begin
		set @body = cast( (
			select td = cast( id as varchar(30))  + '</td><td>' + cast( domain_name as varchar(100)) + '</td><td>' + cast( table_name as varchar(150)) + '</td><td>'
			+ cast( isnull(partition_name,'') as varchar(100)) + '</td><td>' 
			+ case when (select count(1) from metadata.error_log e with(nolock) 
			where source_id=@paramSpSourceId and batch_id=@paramSpBatchId and e.job_object_id=d.id)>0 then 'Failed' 
			when (select count(1) from metadata.[audit_log] a with(nolock) where source_id=@paramSpSourceId and batch_id=@paramSpBatchId and a.job_object_id=d.id)>0 then 'Failed' 
			else 'Skipped' end
			+ '</td><td>'
			+ isnull((select top 1 convert(varchar(25),error_log_id) from metadata.error_log e with(nolock) 
		where source_id=@paramSpSourceId and batch_id=@paramSpBatchId and e.job_object_id=d.id order by log_date),'')
 			from (					
				Select id,domain_name,table_name,partition_name
				From [metadata].[powerbi_dataset_details] pbi
				Where enabled=1 and id not in (SELECT jod.id FROM metadata.[powerbi_dataset_details] jod with(nolock)
				inner join metadata.audit_log a with(nolock) on jod.id = a.job_object_id and a.batch_id =@paramSpBatchId			
				WHERE jod.enabled=1 and isnull(a.status,'') in ('success'))	
			  ) as d			 
		for xml path( 'tr' ), type ) as varchar(max) )	  	 
	end
	else
	begin	
		set @body = cast( (
			select td = cast( id as varchar(30))  + '</td><td>' + cast( domain_name as varchar(100)) + '</td><td>' + cast( table_name as varchar(150)) + '</td><td>'
			+ cast( isnull(partition_name,'') as varchar(100)) + '</td><td>' 
			+ case when (select count(1) from metadata.error_log e with(nolock) 
			where source_id=@paramSpSourceId and batch_id=@paramSpBatchId and e.job_object_id=d.id)>0 then 'Failed' 
			when (select count(1) from metadata.[audit_log] a with(nolock) where source_id=@paramSpSourceId and batch_id=@paramSpBatchId and a.job_object_id=d.id)>0 then 'Failed' 
			else 'Skipped' end
			+ '</td><td>'
			+ isnull((select top 1 convert(varchar(25),error_log_id) from metadata.error_log e with(nolock) 
		where source_id=@paramSpSourceId and batch_id=@paramSpBatchId and e.job_object_id=d.id order by log_date),'')
 			from (					
				Select id,domain_name,table_name,partition_name
				From [metadata].[powerbi_dataset_details] pbi
				Where enabled=1 and id not in (SELECT jod.id FROM metadata.[powerbi_dataset_details] jod with(nolock)
				inner join metadata.audit_log a with(nolock) on jod.id = a.job_object_id and a.batch_id =@paramSpBatchId			
				WHERE jod.enabled=1 and isnull(a.status,'') in ('success'))	and 
				(partition_name is null or concat(SUBSTRING(partition_name,1,4),RIGHT(Replicate('0', 2) + SUBSTRING(partition_name,6,LEN(partition_name)),2)) > = FORMAT(dateadd(month, -1, GETDATE()), 'yyyyMM'))
			  ) as d			 
		for xml path( 'tr' ), type ) as varchar(max) )	  
	end

		set @body = @bodyheader + '<table cellpadding=''2'' cellspacing=''2'' border=''1''>'
				+ '<tr><th>Id</th><th>Domain Name</th><th>Table Name</th><th>Partition Name</th><th>Isfailed</th><th>ErrorLogId</th></tr>'
				+ replace( replace( @body, '&lt;', '<' ), '&gt;', '>' )
				+ '</table>'
	 
		set @body=@body +'<br> Please check error log table for more deatils <br>'	
	
	end
	print(@body)
	Select @totaltablecount=LEN(isnull(@body,' '))
	If @totaltablecount = 0
	begin
	    set @body ='Hi Team, <br/><br/> There is no failure records for current execution -- SourceID: '+convert(varchar(50),@paramSpSourceId)+' , BatchID: '+convert(varchar(50),@paramSpBatchId)+'<br/><br/>'	 
	end
end 

declare @subject varchar(max),@servername varchar(255)

set @body=@body +'<br> Thanks, <br> Data Team'
select @servername=@@SERVERNAME
set @subject='SourceID: '+convert(varchar(50),@paramSpSourceId)+' '+@paramSpTopic+ ' execution '+@paramSpStatus +' status, servername: '+@servername+' - ' + convert(varchar(50),GETUTCDATE())
select topic,tolist,@subject as subject,@body as body,@totaltablecount as totaltablecount from metadata.email_details with(nolock) where topic=@paramSpTopic and status=@paramSpStatus

END TRY
BEGIN CATCH
 DECLARE @activity VARCHAR(50) = ERROR_PROCEDURE(),
          @error_code VARCHAR(100) = ERROR_NUMBER(),
		  @error_type VARCHAR(250) = 'TechnicalError',
		  @error_message VARCHAR(250) = ERROR_MESSAGE(),
		  @log_date date = getdate()         

  EXECUTE [metadata].[prc_update_error_log] NULL,
									     @paramSpSourceId,
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