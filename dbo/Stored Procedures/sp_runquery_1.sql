create PROC [dbo].[sp_runquery] @paramSpQuery [varchar](500) AS
begin
DECLARE @UpdateTmpSQL nvarchar(4000)
SET @UpdateTmpSQL= @paramSpQuery

if (@UpdateTmpSQL not like '%drop%')
	EXECUTE sp_executesql @UpdateTmpSQL
else
	begin 

		declare
			@ErrorMessage varchar(max)= 'query not valid'

		RAISERROR (@ErrorMessage,1,1)
	end
end