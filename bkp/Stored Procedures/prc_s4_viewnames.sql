
CREATE procedure [bkp].[prc_s4_viewnames]
AS

SET NOCOUNT ON;
--declare @viewnames1 varchar(max)
select viewname,loadtype,keycolumns,checkpointkey,filter,target_table_name,target_schema_name from [bkp].[viewname_s4] where enable = 1