CREATE TABLE [metadata].[oraclefilter_details] (
    [segment1]           INT      NULL,
    [segment2]           INT      NULL,
    [last_modified_date] DATETIME DEFAULT (getutcdate()) NOT NULL
);

