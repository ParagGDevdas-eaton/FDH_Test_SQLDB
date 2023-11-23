CREATE proc bkp.test_proc
as

begin try
	select 1/0;
end try 

begin catch

 declare @error_num varchar(10)= ERROR_NUMBER(),
    @error_proc varchar(30) = ERROR_PROCEDURE() ,
	@error_msg varchar(40) = ERROR_MESSAGE()

	print('Error details')
	print(@error_num)
	print(@error_proc)
	print(@error_msg);

	throw 50001,@error_msg,1;
end catch