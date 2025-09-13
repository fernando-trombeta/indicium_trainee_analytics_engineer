with
    stg_transacoes as (
        select *
        from {{ ref('stg_transacoes') }}
    )

    /* Remove possíveis duplicados */
    , dedup as (
        select
            *
            , row_number() over (
                partition by id_transacao
                order by data_transacao desc
            ) as rn
        from stg_transacoes
    )

    /* Gera a surrogate key */
    , surrogate_key as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_transacao']) }} as sk_transacao
            , *
        from dedup
        where rn = 1
    )

    /* Enriquece os dados */
    , enriched as (
        select
            sk_transacao
            , id_transacao
            , numero_conta
            , data_transacao
            , extract(year from data_transacao) as ano_transacao
            , extract(month from data_transacao) as mes_transacao
            , nome_transacao
            , valor_transacao
        from surrogate_key
    )

select *
from enriched