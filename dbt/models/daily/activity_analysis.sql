
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

select
    repository_id,
    date(datetime_updated) as date,
    count(repository_id) as number_of_activities
from {{source("github_profile", "REPOSITORY_ACTIVITY")}}
group by repository_id, date(datetime_updated)
order BY
    date(datetime_updated) desc,
    count(repository_id) DESC


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
