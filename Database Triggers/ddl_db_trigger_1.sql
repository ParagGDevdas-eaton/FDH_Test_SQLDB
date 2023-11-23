create TRIGGER [ddl_db_trigger]
ON DATABASE
FOR CREATE_FUNCTION,
CREATE_PROCEDURE,
CREATE_TABLE,
CREATE_TRIGGER,
CREATE_VIEW,
ALTER_FUNCTION,
ALTER_PROCEDURE,
ALTER_TABLE,
ALTER_TRIGGER,
ALTER_VIEW,
DROP_FUNCTION,
DROP_PROCEDURE,
DROP_TABLE,
DROP_TRIGGER,
DROP_VIEW,
CREATE_INDEX,
ALTER_INDEX,
DROP_INDEX
AS
BEGIN

set nocount ON

--DELETE [metadata].[Schema_Changes_Track] WHERE convert(date,[Date]) < convert(date,GETDATE()-365)

insert into [metadata].[Schema_Changes_Track]([Login_Name],[Program_Name],[Host_Name],[Event_Type],[Server_Name],[Database_Name],[Command_Text])
select login_name, program_name, host_name, CONVERT(VARCHAR(215), EVENTDATA().query('data(/EVENT_INSTANCE/EventType)')) as event_type,
CONVERT(VARCHAR(225), EVENTDATA().query('data(/EVENT_INSTANCE/ServerName)')) as server_name,
CONVERT(VARCHAR(225), EVENTDATA().query('data(/EVENT_INSTANCE/DatabaseName)')) as database_name,
REPLACE(CONVERT(VARCHAR(MAX), EVENTDATA().query('data(/EVENT_INSTANCE/TSQLCommand/CommandText)')),'&#x0D;','') as command_text
from sys.dm_exec_sessions WITH(NOLOCK) where session_id=@@SPID

set nocount OFF
END
