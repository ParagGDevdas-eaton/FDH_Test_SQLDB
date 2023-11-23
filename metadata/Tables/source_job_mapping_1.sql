CREATE TABLE [metadata].[source_job_mapping] (
    [source_job_id]      INT          IDENTITY (1, 1) NOT NULL,
    [source_id]          INT          NOT NULL,
    [job_id]             INT          NOT NULL,
    [dependent_on]       INT          NULL,
    [enabled]            BIT          NULL,
    [last_modified_date] DATETIME     DEFAULT (getutcdate()) NULL,
    [current_status]     VARCHAR (50) NULL,
    CONSTRAINT [PK_source_job_mapping] PRIMARY KEY CLUSTERED ([source_job_id] ASC),
    CHECK ([current_status]='Not Started')
);

