CREATE TABLE [metadata].[error_log] (
    [error_log_id]  INT            IDENTITY (1, 1) NOT NULL,
    [trigger_id]    VARCHAR (500)  NULL,
    [batch_id]      BIGINT         NULL,
    [source_iD]     INT            NULL,
    [job_iD]        INT            NULL,
    [job_object_id] INT            NULL,
    [activity]      VARCHAR (500)  NULL,
    [error_code]    VARCHAR (500)  NULL,
    [error_type]    VARCHAR (500)  NULL,
    [error_subtype] VARCHAR (250)  NULL,
    [error_message] NVARCHAR (MAX) NULL,
    [log_date]      DATETIME       NULL,
    [notebook_name] VARCHAR (200)  NULL,
    [method_name]   VARCHAR (200)  NULL,
    CONSTRAINT [pk_error_log] PRIMARY KEY CLUSTERED ([error_log_id] ASC),
    CONSTRAINT [error_subtype] CHECK ([error_subtype]='ConfigurationError' OR [error_subtype]='MetadataError' OR [error_subtype]='SchemaBindingError' OR [error_subtype]='ReadStreamError' OR [error_subtype]='WriteStreamError' OR [error_subtype]='SPError' OR [error_subtype]='NullCheckError' OR [error_subtype]='DuplicateCheckError' OR [error_subtype]='LengthCheckError' OR [error_subtype]='TypeCheckError' OR [error_subtype]='RangeError' OR [error_subtype]='LogicalError'),
    CONSTRAINT [error_type] CHECK ([error_type]='UserError' OR [error_type]='TechnicalError' OR [error_type]='DQCheckError')
);

