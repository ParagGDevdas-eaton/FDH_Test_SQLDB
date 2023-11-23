CREATE TABLE [metadata].[job_object_details] (
    [job_object_id]                 INT            IDENTITY (1, 1) NOT NULL,
    [job_id]                        INT            NOT NULL,
    [source_id]                     INT            NOT NULL,
    [source_job_id]                 INT            NULL,
    [object_id]                     INT            NULL,
    [source_container_name]         VARCHAR (1000) NULL,
    [source_object_path]            VARCHAR (1000) NULL,
    [source_object_name]            VARCHAR (255)  NULL,
    [source_query]                  VARCHAR (MAX)  NULL,
    [source_filter]                 VARCHAR (MAX)  NULL,
    [source_sheet_name]             VARCHAR (255)  NULL,
    [source_object_functional_name] VARCHAR (255)  NULL,
    [source_format]                 VARCHAR (255)  NULL,
    [source_object_delimiter]       VARCHAR (255)  NULL,
    [source_partitioned_by]         VARCHAR (255)  NULL,
    [source_update_identifier]      VARCHAR (255)  NULL,
    [source_update_identifier_hist] VARCHAR (255)  NULL,
    [source_delete_identifier]      VARCHAR (255)  NULL,
    [target_container_name]         VARCHAR (1000) NULL,
    [target_object_path]            VARCHAR (1000) NULL,
    [target_object_name]            VARCHAR (1000) NULL,
    [target_table_type]             VARCHAR (255)  NULL,
    [target_format]                 VARCHAR (255)  NULL,
    [target_object_delimiter]       VARCHAR (255)  NULL,
    [target_partitioned_by]         VARCHAR (255)  NULL,
    [enabled]                       INT            NULL,
    [load_type]                     VARCHAR (255)  NULL,
    [rejected_object_path]          VARCHAR (255)  NULL,
    [common_log_object_path]        VARCHAR (255)  NULL,
    [common_log_object_name]        VARCHAR (255)  NULL,
    [last_modified_date]            DATETIME       DEFAULT (getutcdate()) NULL,
    [is_absolute_path]              INT            NULL,
    [method_name]                   VARCHAR (500)  NULL,
    [dq_check_flag]                 INT            NULL,
    [is_full_load]                  INT            NULL,
    [is_full_load_hist]             INT            NULL,
    [last_run_time]                 DATETIME       NULL,
    [Source_column_mapping]         VARCHAR (255)  NULL,
    [dependent_table_list]          VARCHAR (8000) NULL,
    [execution_sequence]            INT            NULL,
    [source_filter_add]             VARCHAR (8000) NULL,
    CONSTRAINT [PK_job_object_details] PRIMARY KEY CLUSTERED ([job_object_id] ASC)
);


GO


/****** Object:  Trigger [trg_audit_job_object_details]    Script Date: 31-05-2023 00:17:40 ******/

CREATE trigger [metadata].[trg_audit_job_object_details]
on [metadata].[job_object_details]
after UPDATE, INSERT, DELETE
as
declare @user varchar(255), @activity varchar(20);
SET @user = SYSTEM_USER;
if exists(SELECT * from inserted) and exists (SELECT * from deleted)
begin
    SET @activity = 'UPDATE';    
	INSERT into [metadata].[job_object_details_audit]([job_object_id],[job_id],[source_id],[source_job_id],[object_id],[source_container_name],[source_object_path],[source_object_name],[source_query],[source_filter],[source_sheet_name],[source_object_functional_name],[source_format],
	[source_object_delimiter],[source_partitioned_by],[source_update_identifier],[source_update_identifier_hist],[source_delete_identifier],[target_container_name],[target_object_path],[target_object_name],
	[target_table_type],[target_format],[target_object_delimiter],[target_partitioned_by],[enabled],[load_type],[rejected_object_path],[common_log_object_path],[common_log_object_name],[last_modified_date],
	[is_absolute_path],[method_name],[dq_check_flag],[is_full_load],[is_full_load_hist],[last_run_time],dependent_table_list,[execution_sequence],Activity, change_doneBy,modify_date,source_filter_add) 
	select [job_object_id],[job_id],[source_id],[source_job_id],[object_id],[source_container_name],[source_object_path],[source_object_name],[source_query],[source_filter],[source_sheet_name],[source_object_functional_name],[source_format],
	[source_object_delimiter],[source_partitioned_by],[source_update_identifier],[source_update_identifier_hist],[source_delete_identifier],[target_container_name],[target_object_path],[target_object_name],
	[target_table_type],[target_format],[target_object_delimiter],[target_partitioned_by],[enabled],[load_type],[rejected_object_path],[common_log_object_path],[common_log_object_name],[last_modified_date],
	[is_absolute_path],[method_name],[dq_check_flag],[is_full_load],[is_full_load_hist],[last_run_time],dependent_table_list,[execution_sequence],@activity,@user,getdate(),source_filter_add from inserted i;
end

If exists (Select * from inserted) and not exists(Select * from deleted)
begin
    SET @activity = 'INSERT';
	INSERT into [metadata].[job_object_details_audit]([job_object_id],[job_id],[source_id],[source_job_id],[object_id],[source_container_name],[source_object_path],[source_object_name],[source_query],[source_filter],[source_sheet_name],[source_object_functional_name],[source_format],
	[source_object_delimiter],[source_partitioned_by],[source_update_identifier],[source_update_identifier_hist],[source_delete_identifier],[target_container_name],[target_object_path],[target_object_name],
	[target_table_type],[target_format],[target_object_delimiter],[target_partitioned_by],[enabled],[load_type],[rejected_object_path],[common_log_object_path],[common_log_object_name],[last_modified_date],
	[is_absolute_path],[method_name],[dq_check_flag],[is_full_load],[is_full_load_hist],[last_run_time],dependent_table_list,[execution_sequence],Activity, change_doneBy,modify_date,source_filter_add) 
	select [job_object_id],[job_id],[source_id],[source_job_id],[object_id],[source_container_name],[source_object_path],[source_object_name],[source_query],[source_filter],[source_sheet_name],[source_object_functional_name],[source_format],
	[source_object_delimiter],[source_partitioned_by],[source_update_identifier],[source_update_identifier_hist],[source_delete_identifier],[target_container_name],[target_object_path],[target_object_name],
	[target_table_type],[target_format],[target_object_delimiter],[target_partitioned_by],[enabled],[load_type],[rejected_object_path],[common_log_object_path],[common_log_object_name],[last_modified_date],
	[is_absolute_path],[method_name],[dq_check_flag],[is_full_load],[is_full_load_hist],[last_run_time],dependent_table_list,[execution_sequence],@activity,@user,getdate(),source_filter_add from inserted i;
end

If exists(select * from deleted) and not exists(Select * from inserted)
begin 
    SET @activity = 'DELETE';
	INSERT into [metadata].[job_object_details_audit]([job_object_id],[job_id],[source_id],[source_job_id],[object_id],[source_container_name],[source_object_path],[source_object_name],[source_query],[source_filter],[source_sheet_name],[source_object_functional_name],[source_format],
	[source_object_delimiter],[source_partitioned_by],[source_update_identifier],[source_update_identifier_hist],[source_delete_identifier],[target_container_name],[target_object_path],[target_object_name],
	[target_table_type],[target_format],[target_object_delimiter],[target_partitioned_by],[enabled],[load_type],[rejected_object_path],[common_log_object_path],[common_log_object_name],[last_modified_date],
	[is_absolute_path],[method_name],[dq_check_flag],[is_full_load],[is_full_load_hist],[last_run_time],dependent_table_list,[execution_sequence],Activity, change_doneBy,modify_date,source_filter_add) 
	select [job_object_id],[job_id],[source_id],[source_job_id],[object_id],[source_container_name],[source_object_path],[source_object_name],[source_query],[source_filter],[source_sheet_name],[source_object_functional_name],[source_format],
	[source_object_delimiter],[source_partitioned_by],[source_update_identifier],[source_update_identifier_hist],[source_delete_identifier],[target_container_name],[target_object_path],[target_object_name],
	[target_table_type],[target_format],[target_object_delimiter],[target_partitioned_by],[enabled],[load_type],[rejected_object_path],[common_log_object_path],[common_log_object_name],[last_modified_date],
	[is_absolute_path],[method_name],[dq_check_flag],[is_full_load],[is_full_load_hist],[last_run_time],dependent_table_list,[execution_sequence],@activity,@user,getdate(),source_filter_add from deleted i;
end