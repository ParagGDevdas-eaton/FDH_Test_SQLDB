CREATE TABLE [metadata].[powerbi_dataset_details] (
    [id]                   INT           IDENTITY (1, 1) NOT NULL,
    [domain_name]          VARCHAR (255) NULL,
    [datasetid]            VARCHAR (400) NOT NULL,
    [table_name]           VARCHAR (400) NOT NULL,
    [partition_name]       VARCHAR (400) NULL,
    [load_type]            VARCHAR (255) NULL,
    [last_modified_date]   DATETIME      NOT NULL,
    [enabled]              BIT           NULL,
    [job_frequency]        VARCHAR (10)  NULL,
    [dependent_table_list] VARCHAR (MAX) NULL,
    [maxparallelism]       VARCHAR (255) NULL,
    [recalc_consider]      VARCHAR (255) NULL,
    [execution_sequence]   VARCHAR (1)   NULL,
    [domain_id]            INT           NULL,
    [quarter_name]         VARCHAR (255) NULL,
    [groupid]              VARCHAR (400) NULL
);

