--exec [metadata].[prc_email_details] 2,'sourcetoraw','failed',1,3,1
 CREATE  procedure [metadata].[prc_email_details_fccsdelete]
 @paramSpTopic varchar(255),@paramSpStatus varchar(50),@paramSpDeletedFiles int
AS
BEGIN TRY

SET NOCOUNT ON;
 --this sp is used to send emails after fccs files are deleted from oracle cloud
declare @body varchar(max)
set @body='Hi Team, <br/><br/> Please find '+@paramSpTopic+' execution '+@paramSpStatus+' status '+' FilesDeleted: '+convert(varchar(50),@paramSpDeletedFiles ) + '<br/><br/>'
   
declare @subject varchar(max),@servername varchar(255)
select @servername=@@SERVERNAME
set @subject=@paramSpTopic+ ' execution '+@paramSpStatus +' status, servername: '+@servername+' - ' + convert(varchar(50),GETUTCDATE())
select topic,tolist,@subject as subject,@body as body from metadata.email_details with(nolock) where topic=@paramSpTopic and status=@paramSpStatus
END TRY
BEGIN CATCH
 DECLARE @activity VARCHAR(50) = ERROR_PROCEDURE(),
          @error_code VARCHAR(100) = ERROR_NUMBER(),
		  @error_type VARCHAR(250) = 'TechnicalError',
		  @error_message VARCHAR(250) = ERROR_MESSAGE(),
		  @log_date date = getdate()         

-- update error log
  EXECUTE [metadata].[prc_update_error_log] NULL,
									     NULL,
                                         NULL,
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