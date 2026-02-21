select
    customer_id,
    upper(first_name) as first_name,
    upper(last_name) as last_name,
    email,
    created_at
from {{ source('raw','customers') }}