CREATE Proc [metadata].[prc_email_details_fccsvalidation] @paramSpTopic varchar(255),@paramSpStatus varchar(50), @paramFileCountCheck  varchar(10), @paramFileNameCheck  varchar(max)
AS
BEGIN 

declare @body varchar(max)
declare @subject varchar(max),@servername varchar(255)
select @servername=@@SERVERNAME

If (@paramFileCountCheck = 'true' and len(@paramFileNameCheck)>0)
 begin
	set @subject='FCCS File Name issue '
	set @body = 'Hi Team, <br/><br/> Following FCCS file(s) Name doesn`t match with metadata: <br/><br/> '+ @paramFileNameCheck + '<br/><br/> Please send an email to FCCS team and get it corrected in outbound folder. '
 end

Else if @paramFileCountCheck = 'false'
 begin
	set @subject='FCCS Files not received '
	set @body = 'Hi Team, <br/><br/> We have not received all the FCCS Files for previous month yet. Please send an email to FCCS team and check. '
 end

 Else 
 begin
	set @subject='FCCS Files received and validated'
	set @body = 'Hi Team, <br/><br/> FCCS main pipeline executed'
 end

set @body=@body +'<br><br> Thanks, <br> Data Team'
select topic,tolist,@subject as subject,@body as body from metadata.email_details with(nolock) where topic=@paramSpTopic and status=@paramSpStatus

END