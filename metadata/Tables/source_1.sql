CREATE TABLE [metadata].[source] (
    [source_id]          INT           IDENTITY (1, 1) NOT NULL,
    [source_name]        VARCHAR (255) NULL,
    [source_description] VARCHAR (400) NOT NULL,
    [last_modified_date] DATETIME      DEFAULT (getutcdate()) NOT NULL,
    [enabled]            BIT           NULL,
    CONSTRAINT [PK_source] PRIMARY KEY CLUSTERED ([source_id] ASC)
);

