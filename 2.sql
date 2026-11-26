truncate tbl3;
commit;

insert into tbl3
(attr)

select
  attr
from tbl2
where 1=1;
commit;
