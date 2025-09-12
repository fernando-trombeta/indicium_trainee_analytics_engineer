{{ config(materialized='view') }}
select 1 as id
union all
select 2 as id
