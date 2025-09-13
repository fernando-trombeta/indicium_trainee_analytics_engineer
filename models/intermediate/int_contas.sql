with
    stg_contas as (
        select *
        from {{ ref('stg_contas') }}
    )

    /* Remove possíveis duplicados */
    , dedup as (
        select
            *
            , row_number() over (
                partition by numero_conta
                order by data_ultimo_lancamento_conta desc
            ) as rn
        from stg_contas
    )

    /* Gera a surrogate key */
    , surrogate_key as (
        select
            {{ dbt_utils.generate_surrogate_key(['numero_conta']) }} as sk_contas
            , *
        from dedup
        where rn = 1
    )

    /* Enriquece os dados */
    , enriched as (
        select
            sk_contas
            , numero_conta
            , id_cliente
            , id_agencia
            , id_colaborador
            , tipo_conta
            , data_abertura_conta
            , data_ultimo_lancamento_conta
            , saldo_total_conta
            , saldo_disponivel_conta
            , date_diff(current_date, data_abertura_conta, day) as dias_desde_abertura_conta
        from surrogate_key
    )

select *
from enriched
