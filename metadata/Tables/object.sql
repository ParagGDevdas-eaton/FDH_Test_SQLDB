CREATE TABLE [metadata].[object] (
    [object_id]          INT            IDENTITY (1, 1) NOT NULL,
    [source_id]          INT            NOT NULL,
    [table_name]         VARCHAR (250)  NOT NULL,
    [natural_key_list]   VARCHAR (1000) NULL,
    [last_modified_date] DATETIME       DEFAULT (getutcdate()) NULL,
    CONSTRAINT [PK_object] PRIMARY KEY CLUSTERED ([object_id] ASC)
);

