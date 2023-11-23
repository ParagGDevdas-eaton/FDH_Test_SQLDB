CREATE PROCEDURE [metadata].[prc_getexecutionlist] 
 AS
BEGIN
 
    SET NOCOUNT ON
    Select * from metadata.job_object_details
 
END