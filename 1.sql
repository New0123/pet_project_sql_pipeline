truncate tbl2;
commit;

insert into tbl2
(attr )

select   
  attr
from tbl1
where 1=1;
commit;