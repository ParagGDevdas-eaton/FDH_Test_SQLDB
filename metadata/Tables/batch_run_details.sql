CREATE TABLE [metadata].[batch_run_details] (
    [batch_id]          BIGINT         IDENTITY (1, 1) NOT NULL,
    [source_id]         INT            NOT NULL,
    [job_id]            INT            NULL,
    [source_job_id]     INT            NULL,
    [trigger_id]        VARCHAR (250)  NULL,
    [batch_start_date]  DATETIME       NULL,
    [batch_end_date]    DATETIME       NULL,
    [pipeline_name]     VARCHAR (255)  NULL,
    [batch_log_message] VARCHAR (4000) NULL,
    [batch_run_status]  VARCHAR (255)  NULL,
    [trigger_time]      DATETIME       NULL,
    CONSTRAINT [PK_batch_run_details] PRIMARY KEY CLUSTERED ([batch_id] ASC),
    CONSTRAINT [batch_run_status] CHECK ([batch_run_status]='Active' OR [batch_run_status]='Failed' OR [batch_run_status]='Partially Failed' OR [batch_run_status]='Successfully Completed')
);

