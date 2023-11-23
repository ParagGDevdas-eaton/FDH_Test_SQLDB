CREATE TABLE [metadata].[schema_changes_track] (
    [login_name]         VARCHAR (255) NULL,
    [program_name]       VARCHAR (255) NULL,
    [host_name]          VARCHAR (255) NULL,
    [event_type]         VARCHAR (255) NULL,
    [server_name]        VARCHAR (255) NULL,
    [database_name]      VARCHAR (255) NULL,
    [command_text]       VARCHAR (MAX) NULL,
    [last_modified_date] DATETIME      DEFAULT (getutcdate()) NULL
);

