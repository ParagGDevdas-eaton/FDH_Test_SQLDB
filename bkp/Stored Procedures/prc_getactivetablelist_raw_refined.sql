

--exec [metadata].[prc_getactivetablelist] 4,1,1,'2023-02-21 00:00:00.000',NULL,NULL,115,NULL,0
 
CREATE PROC [bkp].[prc_getactivetablelist_raw_refined] (
@paramSpSourceId int,
@paramSpJobId int,
@paramSpEtlStartdt datetime, 
@paramSpLastRundt datetime, 
@paramSpTableName VARCHAR(MAX),
@paramSpBatchId bigint,
@paramSpCurrentRundt datetime,
@paramSpIsHistLoad int)
as
BEGIN TRY

SET NOCOUNT ON;

--validating sourcid,jobid passed to SP
IF NOT EXISTS(SELECT TOP 1 1 FROM  bkp.job_object_details WITH(NOLOCK) WHERE source_id=@paramSpSourceId and job_id=@paramSpJobId)
	THROW 50010, 'Please pass correct values to parameters source_id, job_id', 1;

--validating batchid should not be null
IF (isnull(@paramSpBatchId,0)=0)
	THROW 50010, 'batch_id cannot be null', 1;

--validating batchid 
IF NOT EXISTS(SELECT TOP 1 1 FROM  metadata.batch_run_details WITH(NOLOCK) WHERE batch_id=@paramSpBatchId)
	THROW 50010, 'Please pass valid batch number', 1;

--validating paramSpIsHistLoad parameter
IF (@paramSpIsHistLoad not in (1,0))
	THROW 50010, 'is_hist_load can only be 1 or 0', 1;

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
IF (@paramSpJobId=1)
BEGIN

	DECLARE @varCurrentRundt datetime
	set @varCurrentRundt=COALESCE(nullIF(@paramSpCurrentRundt,''),@paramSpEtlStartdt AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time')
	DECLARE @VKORG VARCHAR(MAX),@WERKS VARCHAR(MAX),@BUKRS VARCHAR(MAX),@BWKEY VARCHAR(MAX),@segment1 VARCHAR(MAX),@segment2 VARCHAR(MAX) ,@VBELN VARCHAR(MAX)
	--getting SAP filter conditions
	SELECT @VKORG=''''+STRING_AGG(right('0000'+CONVERT(VARCHAR(4),VKORG),4),''',''')+'''',@WERKS=''''+STRING_AGG(right('0000'+CONVERT(VARCHAR(4),WERKS),4),''',''')+'''',@BUKRS=''''+STRING_AGG(right('0000'+CONVERT(VARCHAR(4),BUKRS),4),''',''')+'''',@BWKEY=''''+STRING_AGG(right('0000'+CONVERT(VARCHAR(4),BWKEY),4),''',''')+'''' FROM metadata.sapfilter_details WITH(NOLOCK) 
	--getting oracle filter conditions
	SELECT @segment1=''''+STRING_AGG(right('0000'+CONVERT(VARCHAR(4),segment1),4),''',''')+'''',@segment2=''''+STRING_AGG(right('0000'+CONVERT(VARCHAR(4),segment2),4),''',''')+'''' FROM [metadata].[oraclefilter_details] WITH(NOLOCK) 

	SELECT @VBELN=''''+STRING_AGG(CONVERT(varchar(max), VBELN),''',''')+'''' FROM [metadata].vbrp_historical_records WITH(NOLOCK) 

	 
	IF(@paramSpIsHistLoad=1)
	BEGIN
		SELECT 
		jod.* ,o.table_name,s.source_name,
		case when  UPPER(jod.source_object_name) like '%VBRP%' then
			case when jod.source_query is not null then UPPER(jod.source_query) else 'SELECT * FROM ' + UPPER(jod.source_object_name) end +   -- IF source_query is mentioned in metadata then it will consider sourcequery else it will take select * from table
				case when isnull(source_filter,'') = '' and jod.is_full_load_hist = 0 then ' WHERE ' 
					when isnull(source_filter,'') = '' and jod.is_full_load_hist = 1 then '' 
					when isnull(source_filter,'') <> '' and jod.is_full_load_hist = 0 then ' WHERE ' + UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(source_filter,'@vkorg',@VKORG),'@werks',@WERKS),'@bukrs',@BUKRS),'@bwkey',@BWKEY),'@segment1',@segment1),'@segment2',@segment2)) + ' and '
					when isnull(source_filter,'') <> '' and jod.is_full_load_hist = 1 then ' WHERE ' +  UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(source_filter,'@vkorg',@VKORG),'@werks',@WERKS),'@bukrs',@BUKRS),'@bwkey',@BWKEY),'@segment1',@segment1),'@segment2',@segment2))
					else '' -- IF source_filter is mentioned in metadata table then it will pick filter condition from source filter and filter values will be replaced from filter tables
				end +
				case when jod.is_full_load_hist = 0 and s.source_id= 2 and UPPER(jod.source_update_identifier_hist) in ('RYEAR','GJAHR') then case when FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyy') <> FORMAT(@varCurrentRundt,'yyyy') then UPPER(jod.source_update_identifier_hist) +' >= ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyy') + ''' and '+ UPPER(jod.source_update_identifier_hist) +' <= '''+ FORMAT(@varCurrentRundt,'yyyy') + '''' else UPPER(jod.source_update_identifier_hist) +' = ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyy') + ''''  end	--for SAP
					 when jod.is_full_load_hist = 0 and s.source_id= 2 and UPPER(jod.source_update_identifier_hist) in ('BUDAT','AEDAT','AUDAT','ERDAT','FKDAT','CPUDT') then UPPER(jod.source_update_identifier_hist) + '  >= ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyyMMdd') + ''' and ' + UPPER(jod.source_update_identifier_hist) + ' < '''+ FORMAT(@varCurrentRundt,'yyyyMMdd') + ''''	--for SAP
					 when jod.is_full_load_hist = 0 and s.source_id= 2 and UPPER(jod.source_update_identifier_hist) in ('SLT_UPDATE') then UPPER(jod.source_update_identifier_hist)+ ' >= ' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyyMMddHHmm') + '00.000000 and '+UPPER(jod.source_update_identifier_hist)+' < ' + FORMAT(@varCurrentRundt,'yyyyMMddHHmm') + '00.000000'	--for SAP
					 --when jod.is_full_load_hist = 0 and s.source_id= 1 and jod.source_query is not null then 'A.'+UPPER(jod.source_update_identifier_hist)  + ' >=  TO_DATE(''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'dd-MM-yyyy HH:mm') + ''', ''dd-MM-yyyy HH24:MI'') and A.'+UPPER(jod.source_update_identifier_hist)  + ' < TO_DATE(''' +  FORMAT(@varCurrentRundt, 'dd-MM-yyyy HH:mm') + ''', ''dd-MM-yyyy HH24:MI'')'																			--for Oracle
					 --when jod.is_full_load_hist = 0 and s.source_id= 1 then UPPER(jod.source_update_identifier_hist) + ' >= TO_DATE(''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'dd-MM-yyyy HH:mm') + ''', ''dd-MM-yyyy HH24:MI'') and '+UPPER(jod.source_update_identifier_hist)  + ' < TO_DATE(''' +  FORMAT(@varCurrentRundt, 'dd-MM-yyyy HH:mm') + ''', ''dd-MM-yyyy HH24:MI'')'																			--for Oracle
					 when jod.is_full_load_hist = 0 and s.source_id= 1 and jod.source_query is not null and source_object_name='apps.xla_distribution_links' then ' TO_CHAR(B.'+UPPER(jod.source_update_identifier_hist)  + ', ''yyyy-MM-dd HH24:MI'') >=  ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'yyyy-MM-dd HH:mm') + ''' and TO_CHAR(B.'+UPPER(jod.source_update_identifier_hist)  + ', ''yyyy-MM-dd HH24:MI'') < ''' +  FORMAT(@varCurrentRundt, 'yyyy-MM-dd HH:mm') + ''''
					 when jod.is_full_load_hist = 0 and s.source_id= 1 and jod.source_query is not null then ' TO_CHAR(A.'+UPPER(jod.source_update_identifier_hist)  + ', ''yyyy-MM-dd HH24:MI'') >=  ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'yyyy-MM-dd HH:mm') + ''' and TO_CHAR(A.'+UPPER(jod.source_update_identifier_hist)  + ', ''yyyy-MM-dd HH24:MI'') < ''' +  FORMAT(@varCurrentRundt, 'yyyy-MM-dd HH:mm') + ''''																			--for Oracle
					 when jod.is_full_load_hist = 0 and s.source_id= 1 then ' TO_CHAR('+UPPER(jod.source_update_identifier_hist) + ', ''yyyy-MM-dd HH24:MI'') >= ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'yyyy-MM-dd HH:mm') + ''' and TO_CHAR('+UPPER(jod.source_update_identifier_hist)  + ', ''yyyy-MM-dd HH24:MI'') < ''' +  FORMAT(@varCurrentRundt, 'yyyy-MM-dd HH:mm') + ''''																			--for Oracle

					 when jod.is_full_load_hist = 0 and s.source_id= 4 then UPPER(jod.source_update_identifier_hist) + ' >= TO_DATE(''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'dd-MM-yyyy HH:mm') + ''', ''dd-MM-yyyy HH24:MI'') and '+UPPER(jod.source_update_identifier_hist)  + ' < TO_DATE(''' +  FORMAT(@varCurrentRundt, 'dd-MM-yyyy HH:mm') + ''', ''dd-MM-yyyy HH24:MI'')'	--tph
					else '' end +
                ' union all '+ 'SELECT * FROM slt_ods.VBRP WHERE MANDT=''100'' AND WERKS NOT IN ('+@WERKS+') and VBELN in ('+@VBELN+')'

			else 
			case when jod.source_query is not null then UPPER(jod.source_query) else 'SELECT * FROM ' + UPPER(jod.source_object_name) end +   -- IF source_query is mentioned in metadata then it will consider sourcequery else it will take select * from table
				case when isnull(source_filter,'') = '' and jod.is_full_load_hist = 0 then ' WHERE ' 
					when isnull(source_filter,'') = '' and jod.is_full_load_hist = 1 then '' 
					when isnull(source_filter,'') <> '' and jod.is_full_load_hist = 0 then ' WHERE ' + UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(source_filter,'@vkorg',@VKORG),'@werks',@WERKS),'@bukrs',@BUKRS),'@bwkey',@BWKEY),'@segment1',@segment1),'@segment2',@segment2)) + ' and '
					when isnull(source_filter,'') <> '' and jod.is_full_load_hist = 1 then ' WHERE ' +  UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(source_filter,'@vkorg',@VKORG),'@werks',@WERKS),'@bukrs',@BUKRS),'@bwkey',@BWKEY),'@segment1',@segment1),'@segment2',@segment2))
					else '' -- IF source_filter is mentioned in metadata table then it will pick filter condition from source filter and filter values will be replaced from filter tables
				end +
				case when jod.is_full_load_hist = 0 and s.source_id= 2 and UPPER(jod.source_update_identifier_hist) in ('RYEAR','GJAHR') then case when FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyy') <> FORMAT(@varCurrentRundt,'yyyy') then UPPER(jod.source_update_identifier_hist) +' >= ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyy') + ''' and '+ UPPER(jod.source_update_identifier_hist) +' <= '''+ FORMAT(@varCurrentRundt,'yyyy') + '''' else UPPER(jod.source_update_identifier_hist) +' = ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyy') + ''''  end	--for SAP
					 when jod.is_full_load_hist = 0 and s.source_id= 2 and UPPER(jod.source_update_identifier_hist) in ('BUDAT','AEDAT','AUDAT','ERDAT','FKDAT','CPUDT') then UPPER(jod.source_update_identifier_hist) + '  >= ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyyMMdd') + ''' and ' + UPPER(jod.source_update_identifier_hist) + ' < '''+ FORMAT(@varCurrentRundt,'yyyyMMdd') + ''''	--for SAP
					 when jod.is_full_load_hist = 0 and s.source_id= 2 and UPPER(jod.source_update_identifier_hist) in ('SLT_UPDATE') then UPPER(jod.source_update_identifier_hist)+ ' >= ' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyyMMddHHmm') + '00.000000 and '+UPPER(jod.source_update_identifier_hist)+' < ' + FORMAT(@varCurrentRundt,'yyyyMMddHHmm') + '00.000000'	--for SAP
					 when jod.is_full_load_hist = 0 and s.source_id= 1 and jod.source_query is not null and source_object_name='apps.xla_distribution_links' then ' TO_CHAR(B.'+UPPER(jod.source_update_identifier_hist)  + ', ''yyyy-MM-dd HH24:MI'') >=  ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'yyyy-MM-dd HH:mm') + ''' and TO_CHAR(B.'+UPPER(jod.source_update_identifier_hist)  + ', ''yyyy-MM-dd HH24:MI'') < ''' +  FORMAT(@varCurrentRundt, 'yyyy-MM-dd HH:mm') + ''''
					 when jod.is_full_load_hist = 0 and s.source_id= 1 and jod.source_query is not null then ' TO_CHAR(A.'+UPPER(jod.source_update_identifier_hist)  + ', ''yyyy-MM-dd HH24:MI'') >=  ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'yyyy-MM-dd HH:mm') + ''' and TO_CHAR(A.'+UPPER(jod.source_update_identifier_hist)  + ', ''yyyy-MM-dd HH24:MI'') < ''' +  FORMAT(@varCurrentRundt, 'yyyy-MM-dd HH:mm') + ''''																			--for Oracle
					 when jod.is_full_load_hist = 0 and s.source_id= 1 then ' TO_CHAR('+UPPER(jod.source_update_identifier_hist) + ', ''yyyy-MM-dd HH24:MI'') >= ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'yyyy-MM-dd HH:mm') + ''' and TO_CHAR('+UPPER(jod.source_update_identifier_hist)  + ', ''yyyy-MM-dd HH24:MI'') < ''' +  FORMAT(@varCurrentRundt, 'yyyy-MM-dd HH:mm') + ''''																			--for Oracle

					 when jod.is_full_load_hist = 0 and s.source_id= 4 then UPPER(jod.source_update_identifier_hist) + ' >= TO_DATE(''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'dd-MM-yyyy HH:mm') + ''', ''dd-MM-yyyy HH24:MI'') and '+UPPER(jod.source_update_identifier_hist)  + ' < TO_DATE(''' +  FORMAT(@varCurrentRundt, 'dd-MM-yyyy HH:mm') + ''', ''dd-MM-yyyy HH24:MI'')'	--tph
					else '' end
			end as source_filter_final -- IF it is historical load then historical data will be loaded based on historical column
		,'/' + lower(jod.target_object_path) +'/' + FORMAT(@paramSpEtlStartdt,'yyyyMMdd_HHmm') +'/' as target_object_path_final
		,isnull(t.threshold,0)  as threshold,
		case when UPPER(jod.source_update_identifier_hist) in ('BUDAT','FKDAT','AEDAT','AUDAT') then DATEADD(MONTH, -1, @varCurrentRundt) else @varCurrentRundt end as varCurrentRundt
		FROM bkp.job_object_details  jod  WITH(NOLOCK) 
		inner join metadata.source s  WITH(NOLOCK) on jod.source_id = s.source_id
		inner join metadata.job j  WITH(NOLOCK) on jod.job_id = j.job_id
		inner join metadata.object o  WITH(NOLOCK) on o.object_id=jod.object_id
		left join metadata.sap_filters_threshold t with(nolock) on t.table_name=o.table_name

		WHERE j.job_id=@paramSpJobId and s.source_id =@paramSpSourceId 
			and jod.enabled=1 and s.enabled=1 and j.enabled=1 
			and (
			(isnull(@paramSpTableName,'') <> '' and o.table_name in (SELECT value FROM string_split(@paramSpTableName,','))) -- IF tablesnames are passed then data from only those table list will be reflected
			OR
			(isnull(@paramSpTableName,'') = '' and 
			jod.job_object_id not in (SELECT jod.job_object_id FROM bkp.job_object_details  jod  WITH(NOLOCK) 
			left join metadata.audit_log a  WITH(NOLOCK) on jod.job_object_id = a.job_object_id and a.batch_id=@paramSpBatchId 
			WHERE jod.job_id=@paramSpJobId and jod.source_id =@paramSpSourceId  and jod.enabled=1 and isnull(a.status,'') in ('success')))
			) --IF it is new batch then all enabled tables for that source will be shown else IF it is existing batch then only failed list will be shown
	END
	ELSE IF(@paramSpIsHistLoad=0)
	BEGIN
		SELECT 
		jod.* ,o.table_name,s.source_name
		,case when jod.source_query is not null then UPPER(jod.source_query) else 'SELECT * FROM ' + UPPER(jod.source_object_name) end  +
			case when isnull(source_filter,'') = '' and jod.is_full_load = 0 then ' WHERE ' 
				when isnull(source_filter,'') = '' and jod.is_full_load = 1 then '' 
				when isnull(source_filter,'') <> '' and jod.is_full_load = 0 then ' WHERE ' + UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(source_filter,'@vkorg',@VKORG),'@werks',@WERKS),'@bukrs',@BUKRS),'@bwkey',@BWKEY),'@segment1',@segment1),'@segment2',@segment2)) + ' and '
				when isnull(source_filter,'') <> '' and jod.is_full_load = 1 then ' WHERE ' +  UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(source_filter,'@vkorg',@VKORG),'@werks',@WERKS),'@bukrs',@BUKRS),'@bwkey',@BWKEY),'@segment1',@segment1),'@segment2',@segment2))
				else ''  -- IF source_filter is mentioned in metadata table then it will pick filter condition from source filter and filter values will be replaced from filter tables
			end +
			case when jod.is_full_load = 0 and s.source_id= 2 then UPPER(jod.source_update_identifier)+ ' >= ' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyyMMddHHmm') + '00.000000 and '+UPPER(jod.source_update_identifier)+' < ' + FORMAT(@varCurrentRundt,'yyyyMMddHHmm') + '00.000000'	--for SAP
				 when jod.is_full_load = 0 and s.source_id= 1 and jod.source_query is not null and source_object_name='apps.xla_distribution_links' then ' TO_CHAR(B.'+UPPER(jod.source_update_identifier)  + ', ''yyyy-MM-dd HH24:MI'') >=  ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'yyyy-MM-dd HH:mm') + ''' and TO_CHAR(B.'+UPPER(jod.source_update_identifier)  + ', ''yyyy-MM-dd HH24:MI'') < ''' +  FORMAT(@varCurrentRundt, 'yyyy-MM-dd HH:mm') + ''''																			--for Oracle
				 when jod.is_full_load = 0 and s.source_id= 1 and jod.source_query is not null then ' TO_CHAR(A.'+UPPER(jod.source_update_identifier)  + ', ''yyyy-MM-dd HH24:MI'') >=  ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'yyyy-MM-dd HH:mm') + ''' and TO_CHAR(A.'+UPPER(jod.source_update_identifier)  + ', ''yyyy-MM-dd HH24:MI'') < ''' +  FORMAT(@varCurrentRundt, 'yyyy-MM-dd HH:mm') + ''''																			--for Oracle
				 when jod.is_full_load = 0 and s.source_id= 1 then ' TO_CHAR('+UPPER(jod.source_update_identifier) + ', ''yyyy-MM-dd HH24:MI'') >= ''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'yyyy-MM-dd HH:mm') + ''' and TO_CHAR('+UPPER(jod.source_update_identifier)  + ', ''yyyy-MM-dd HH24:MI'') < ''' +  FORMAT(@varCurrentRundt, 'yyyy-MM-dd HH:mm') + ''''																			--for Oracle

				 when jod.is_full_load = 0 and s.source_id= 4 then UPPER(jod.source_update_identifier) + ' >=  TO_DATE(''' + FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time), 'dd-MM-yyyy HH:mm') + ''', ''dd-MM-yyyy HH24:MI'') and '+ UPPER(jod.source_update_identifier)  + ' < TO_DATE(''' +  FORMAT(@varCurrentRundt, 'dd-MM-yyyy HH:mm') + ''', ''dd-MM-yyyy HH24:MI'')'	--tph

				else '' end as source_filter_final 				-- IF it is historical load then historical data will be loaded based on historical column  
		,'/' + lower(jod.target_object_path) +'/' + FORMAT(@paramSpEtlStartdt,'yyyyMMdd_HHmm') +'/' as target_object_path_final
		,isnull(t.threshold,0) as threshold
		,@varCurrentRundt as varCurrentRundt
		FROM bkp.job_object_details  jod  WITH(NOLOCK) 
		inner join metadata.source s  WITH(NOLOCK) on jod.source_id = s.source_id
		inner join metadata.job j WITH(NOLOCK) on jod.job_id = j.job_id
		inner join metadata.object o WITH(NOLOCK) on o.object_id=jod.object_id
		left join metadata.sap_filters_threshold t with(nolock) on t.table_name=o.table_name

		WHERE j.job_id=@paramSpJobId and s.source_id =@paramSpSourceId 
			and jod.enabled=1 and s.enabled=1 and j.enabled=1 
			and (
			(isnull(@paramSpTableName,'') <> '' and o.table_name in (SELECT value FROM string_split(@paramSpTableName,','))) -- IF tablesnames are passed then data from only those table list will be reflected
			OR
			(isnull(@paramSpTableName,'') = '' and jod.job_object_id not in (SELECT jod.job_object_id FROM bkp.job_object_details  jod with(nolock)
			left join metadata.audit_log a with(nolock) on jod.job_object_id = a.job_object_id and a.batch_id=@paramSpBatchId 
			WHERE jod.job_id=@paramSpJobId and jod.source_id =@paramSpSourceId and jod.enabled=1 and isnull(a.status,'') in ('success'))
			))--IF it is new batch then all enabled tables for that source will be shown else IF it is existing batch then only failed list will be shown
	END
END
ELSE IF(@paramSpJobId=3)
BEGIN

	SELECT 
	jod.* ,o.table_name,s.source_name,
	SUBSTRING(jod.target_object_name,  1, CHARINDEX('.', jod.target_object_name)-1) target_schema_name,
	SUBSTRING(jod.target_object_name, CHARINDEX('.', jod.target_object_name)+1, LEN(jod.target_object_name)) target_table_name,
 	FORMAT(COALESCE(nullIF(@paramSpLastRundt,''),jod.last_run_time),'yyyy-MM-dd HH:mm:ss.fff') as valid_last_run_time,
    FORMAT(GETUTCDATE(),'yyyy-MM-dd HH:mm:ss.fff') as valid_current_run_time
	FROM bkp.job_object_details  jod  WITH(NOLOCK) 
	inner join metadata.source s  WITH(NOLOCK) on jod.source_id = s.source_id
	inner join metadata.job j  WITH(NOLOCK) on jod.job_id = j.job_id
	inner join metadata.object o  WITH(NOLOCK) on o.object_id=jod.object_id

	WHERE j.job_id=@paramSpJobId and s.source_id =@paramSpSourceId 
	    and jod.enabled=1 and s.enabled=1 and j.enabled=1 
		and (--- If table list is provided
				(isnull(@paramSpTableName,'') <> '' and o.table_name in (SELECT value FROM string_split(@paramSpTableName,',')))
			OR
			---- Fetch active table list and whose DQ is completed 
			(isnull(@paramSpTableName,'') = '' 
				and jod.job_object_id not in 
				(SELECT jod.job_object_id FROM bkp.job_object_details  jod with(nolock)
					left join metadata.audit_log a with(nolock) on jod.job_object_id = a.job_object_id and a.batch_id=@paramSpBatchId 
					WHERE jod.job_id=@paramSpJobId and jod.source_id =@paramSpSourceId  and jod.enabled=1 and isnull(a.status,'') in ('success')
				)
		and ---DQ Check
			(jod.object_id in (SELECT distinct jod.object_id FROM bkp.job_object_details  jod  WITH(NOLOCK) 
			left join metadata.audit_log a  WITH(NOLOCK) on jod.job_object_id = a.job_object_id and a.batch_id=@paramSpBatchId 
			WHERE jod.job_id=2 and jod.source_id =@paramSpSourceId 
			and jod.enabled=1 and isnull(a.status,'') in ('success')))
			)	)

END
END TRY
--end
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