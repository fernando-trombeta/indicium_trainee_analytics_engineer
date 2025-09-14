with
    int_agencias as (
        select *
        from {{ ref('int_agencias') }}
    )

    , final as (
        select
            sk_agencias
            , id_agencia
            , nome_agencia
            , cidade_agencia
            , uf_agencia
            , regiao_agencia
            , data_abertura_agencia
            , anos_atividade_agencia
            , tipo_agencia
        from int_agencias
    )

select *
from final