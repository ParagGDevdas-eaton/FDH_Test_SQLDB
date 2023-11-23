
create proc metadata.prc_getactivefccsfilelist
as
begin
	select distinct case when source_object_name like 'ExportedMetadata_%' 
		then replace(replace(source_object_path,'fccs_files/','') ,'.zip','') else source_object_name end filename
	from metadata.job_object_details where job_id=1 and source_id=3 and enabled=1

end