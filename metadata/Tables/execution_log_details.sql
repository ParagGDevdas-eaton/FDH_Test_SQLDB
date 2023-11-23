CREATE TABLE [metadata].[execution_log_details] (
    [log_id]        INT           IDENTITY (1, 1) NOT NULL,
    [trigger_id]    VARCHAR (500) NULL,
    [batch_id]      BIGINT        NULL,
    [job_id]        INT           NULL,
    [job_object_id] INT           NULL,
    [step_name]     VARCHAR (500) NULL,
    [start_time]    DATETIME      NULL,
    [end_time]      DATETIME      NULL,
    [row_count]     BIGINT        NULL
);

