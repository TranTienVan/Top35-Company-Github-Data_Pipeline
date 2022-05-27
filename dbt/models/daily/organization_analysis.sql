
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

select 
    o.name,
    o.html_url,
    o.blog,
    o.location,
    count(om.user_id) as number_of_members,
    count(r.id) as number_of_repositories
from {{source("github_profile", "USER")}} as o
inner join {{source("github_profile", "ORGANIZATION_MEMBER")}} as om
    on o.id = om.organization_id
inner join {{source("github_profile", "REPOSITORY")}} as r
    on r.organization_id = o.id
where o.type = 'Organization'
group by 
    o.name, 
    o.html_url, 
    o.blog, 
    o.location
order by 
    count(om.user_id) desc, 
    count(r.id) desc


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
