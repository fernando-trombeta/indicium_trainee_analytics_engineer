with
    stg_propostas_credito as (
        select *
        from {{ ref('stg_propostas_credito') }}
    )

    /* Remove possíveis duplicados */
    , dedup as (
        select
            *
            , row_number() over (
                partition by id_proposta
                order by data_entrada_proposta desc
            ) as rn
        from stg_propostas_credito
    )

    /* Gera a surrogate key */
    , surrogate_key as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_proposta']) }} as sk_propostas_credito
            , *
        from dedup
        where rn = 1
    )

    /* Enriquece os dados */
    , final as (
        select
            sk_propostas_credito
            , id_proposta
            , id_cliente
            , id_colaborador
            , data_entrada_proposta
            , status_proposta
            , taxa_juros_mensal
            , valor_proposta
            , valor_financiamento
            , valor_entrada
            , valor_prestacao
            , quantidade_parcelas
            , carencia
        from surrogate_key
    )

select *
from final