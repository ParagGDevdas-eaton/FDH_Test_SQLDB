CREATE TABLE [metadata].[email_details] (
    [topic]         VARCHAR (1000) NULL,
    [tolist]        VARCHAR (2000) NULL,
    [cclist]        VARCHAR (2000) NULL,
    [subject]       VARCHAR (2000) NULL,
    [body]          VARCHAR (8000) NULL,
    [status]        VARCHAR (100)  NULL,
    [modified_time] DATETIME       DEFAULT (getutcdate()) NULL
);

