CREATE TABLE [bkp].[recon_fa_sap_records] (
    [domain]                          VARCHAR (2)     NULL,
    [le_number]                       NVARCHAR (2000) NULL,
    [cum_acquisition_value_rpt]       DECIMAL (38, 7) NULL,
    [cum_acquisition_value_certified] DECIMAL (38, 7) NULL,
    [cum_acquisition_value_refined]   DECIMAL (38, 7) NULL,
    [cum_acquisition_value_match]     VARCHAR (1)     NULL,
    [accum_depreciation_rpt]          DECIMAL (38, 7) NULL,
    [accum_depreciation_certified]    DECIMAL (38, 7) NULL,
    [accum_depreciation_refined]      DECIMAL (38, 7) NULL,
    [accum_depreciation_match]        VARCHAR (1)     NULL,
    [depreciation_rpt]                DECIMAL (38, 7) NULL,
    [depreciation_certified]          DECIMAL (38, 7) NULL,
    [depreciation_refined]            DECIMAL (38, 7) NULL,
    [depreciation_match]              VARCHAR (1)     NULL
);

