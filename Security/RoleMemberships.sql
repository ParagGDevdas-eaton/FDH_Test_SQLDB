ALTER ROLE [db_owner] ADD MEMBER [eaaz_it_finance_data_hub_dev_sqldb_dbowner];


GO
ALTER ROLE [db_owner] ADD MEMBER [fdhsyneusdev002];


GO
ALTER ROLE [db_ddladmin] ADD MEMBER [eaaz_it_finance_data_hub_dev_sqldb_developer];


GO
ALTER ROLE [db_datareader] ADD MEMBER [eaaz_it_finance_data_hub_dev_sqldb_dbread];


GO
ALTER ROLE [db_datareader] ADD MEMBER [eaaz_it_finance_data_hub_dev_sqldb_developer];


GO
ALTER ROLE [db_datawriter] ADD MEMBER [eaaz_it_finance_data_hub_dev_sqldb_developer];

