with 
    source_data as (
        select *
        from {{ ref('colaborador_agencia') }}
)      

    , transformed_data as (
        select
            cast(cod_colaborador as int) as id_colaborador
            , cast(cod_agencia as int) as id_agencia
        from source_data
)

select *
from transformed_data