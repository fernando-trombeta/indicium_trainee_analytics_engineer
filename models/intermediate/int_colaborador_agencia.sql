with
    stg_colaborador_agencia as (
        select * 
        from {{ ref('stg_colaborador_agencia') }}
    )

    /* Remove possíveis duplicados */
    , dedup as (
        select
            *
            , row_number() over (
                partition by id_colaborador, id_agencia
                order by id_colaborador, id_agencia
            ) as rn
        from stg_colaborador_agencia
    )

    /* Gera a surrogate key */
    , surrogate_key as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_colaborador','id_agencia']) }} as sk_colaborador_agencia
            , *
        from dedup
        where rn = 1
    )

    , final as (
        select
            sk_colaborador_agencia
            , id_colaborador
            , id_agencia
        from surrogate_key
    )

select *
from final
