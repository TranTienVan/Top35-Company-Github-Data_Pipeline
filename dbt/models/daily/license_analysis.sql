
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

select
    l.key,
    l.name,
    count(r.id) as number_of_repositores
from {{source("github_profile", "LICENSE")}} as l
inner join {{source("github_profile", "REPOSITORY")}} as r
    on l.key = r.license_key
group BY
    l.key,
    l.name
order by count(r.id) desc



/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
