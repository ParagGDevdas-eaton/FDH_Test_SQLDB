CREATE proc bkp.prc_getactivetablelist
as
begin
	select tableschema,tablename,filename,container,filepath from bkp.lkptable where enable = 1
end