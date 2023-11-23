--select * from bkp.recon_fa_sap_records
--exec bkp.prc_recon_fa_sap_records

CREATE proc bkp.prc_recon_fa_sap_records
AS
Begin
declare @body varchar(max)
declare @bodyheader varchar(max)
declare @totaltablecount varchar(max) 
declare @totaltimeinmin decimal(11,4) 
declare @totaltimeinhr varchar(max)
set @totaltablecount = 0

--declare @body varchar(max)
		set @body = cast( (
		select td = le_number + '</td><td>' + cast( cum_acquisition_value_rpt as varchar(30) ) + '</td><td>' 
		+ cast( cum_acquisition_value_certified as varchar(30))  + '</td><td>' + cast( cum_acquisition_value_refined as varchar(30)) + '</td><td>' + [cum_acquisition_value_match]
		from ( select * from bkp.recon_fa_sap_records
			  ) as d
		for xml path( 'tr' ), type ) as varchar(max) )

		set @body = '<table cellpadding=''2'' cellspacing=''2'' border=''1''>'
				  + '<tr><th>Table Name</th><th>TimeTaken[HH:MM:SS]</th><th>SourceRowCount</th><th>TargetRowCount</th><th>Status</th></tr>'
				  + replace( replace( @body, '&lt;', '<' ), '&gt;', '>' )
				  + '</table>'

  set @body=@body +'<br> Thanks, <br> Data Team'
declare @subject varchar(max),@servername varchar(255)

set @subject='Test Recon Mail'

select @body as body,@subject as subject from metadata.email_details with(nolock)

   print (@body)

   End