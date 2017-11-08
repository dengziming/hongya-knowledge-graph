

select distinct phone from
(
select phone from registerInfo
union all
select phone from contactInfo
union all
select contact_phone as phone from contactInfo
union all
select phone from callDetail
union all
select other_mobile as phone from callDetail
)