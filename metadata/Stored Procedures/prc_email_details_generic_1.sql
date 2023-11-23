--exec [metadata].[prc_email_details_generic] 1,'pipeline','failed'
 CREATE procedure [metadata].[prc_email_details_generic]
@paramSpSourceId int,@paramSpStatus varchar(50),@paramSpTopic varchar(255)
AS
BEGIN

SET NOCOUNT ON;
declare @body nvarchar(max)
set @body ='Hi Team, <br/><br/> Pipeline failed for SourceID: '+convert(varchar(50),@paramSpSourceId)+' <br/><br/>'	 
declare @subject varchar(max),@servername varchar(255)

set @body=@body +'<br> Thanks, <br> Data Team'
select @servername=@@SERVERNAME
set @subject=@paramSpTopic+ ' execution '+@paramSpStatus +' status, servername: '+@servername+' - ' + convert(varchar(50),GETUTCDATE())
select topic,tolist,@subject as subject,@body as body from metadata.email_details with(nolock) where topic=@paramSpTopic and status=@paramSpStatus

END