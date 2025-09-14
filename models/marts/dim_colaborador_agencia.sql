with
    int_colaborador_agencia as (
        select *
        from {{ ref('int_colaborador_agencia') }}
    )

    , final as (
        select
            sk_colaborador_agencia
            , id_colaborador
            , id_agencia
        from int_colaborador_agencia
    )

select *
from final