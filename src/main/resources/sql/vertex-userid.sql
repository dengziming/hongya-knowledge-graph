
select distinct userid from
(
select userid from registerInfo
union all
select userid from loanOrder
)