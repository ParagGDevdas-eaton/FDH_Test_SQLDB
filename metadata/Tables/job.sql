CREATE TABLE [metadata].[job] (
    [job_id]             INT           NOT NULL,
    [job_name]           VARCHAR (255) NOT NULL,
    [job_description]    VARCHAR (255) NULL,
    [last_modified_date] DATETIME      DEFAULT (getutcdate()) NULL,
    [enabled]            BIT           NULL,
    CONSTRAINT [PK_job] PRIMARY KEY CLUSTERED ([job_id] ASC)
);

