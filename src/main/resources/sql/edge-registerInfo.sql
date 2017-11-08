

select
    user_id AS userid,
    mobile AS phone
from
    $table
where
    mobile is not null
    and
    mobile regexp '^1\\d{10}$$'