CREATE PROCEDURE [bkp].[prc_update_s4_errorlog]
(
@paramviewname varchar(100) NULL,
@paramlog_date datetime2 NULL,
@paramerrormessage varchar(1000) NULL
)
AS

SET NOCOUNT ON;
--declare @viewnames1 varchar(max)
insert into bkp.viewname_s4_errorlog values (@paramviewname,@paramlog_date,@paramerrormessage)