CREATE TABLE [bkp].[source_job_Dependency] (
    [dependency_id]           INT            IDENTITY (1, 1) NOT NULL,
    [source_id]               INT            NOT NULL,
    [job_id]                  INT            NOT NULL,
    [object_id]               INT            NULL,
    [job_object_id]           INT            NULL,
    [ExecutionSequence]       INT            NULL,
    [dependent_source_id]     INT            NULL,
    [dependent_job_id]        INT            NULL,
    [dependent_object_id]     INT            NULL,
    [dependent_job_object_id] INT            NULL,
    [dependency]              NVARCHAR (10)  NULL,
    [comment]                 VARCHAR (2000) NULL,
    [last_modified_date]      DATETIME       NULL,
    [enabled]                 BIT            NULL
);

