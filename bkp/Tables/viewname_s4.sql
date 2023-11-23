CREATE TABLE [bkp].[viewname_s4] (
    [viewname]           VARCHAR (100)   NULL,
    [enable]             INT             NULL,
    [loadtype]           VARCHAR (100)   NULL,
    [keycolumns]         VARCHAR (200)   NULL,
    [checkpointkey]      NVARCHAR (2000) NULL,
    [filter]             NVARCHAR (1000) NULL,
    [target_table_name]  VARCHAR (1000)  NULL,
    [target_schema_name] VARCHAR (100)   NULL
);

