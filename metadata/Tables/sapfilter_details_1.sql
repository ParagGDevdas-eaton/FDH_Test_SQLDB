CREATE TABLE [metadata].[sapfilter_details] (
    [VKORG]              INT      NULL,
    [WERKS]              INT      NULL,
    [BUKRS]              INT      NULL,
    [BWKEY]              INT      NULL,
    [last_modified_date] DATETIME DEFAULT (getutcdate()) NOT NULL
);

