with
    stg_agencias as (
        select * from {{ ref('stg_agencias') }}
    )

    /* Remove possíveis duplicados */
    , dedup as (
        select
            *
            , row_number() over (
                partition by id_agencia
                order by data_abertura_agencia desc
            ) as rn
        from stg_agencias
    )

    /* Gera a surrogate key */
    , surrogate_key as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_agencia']) }} as sk_agencias
            , *
        from dedup
        where rn = 1
    )

    /* Enriquece os dados */
    , enriched as (
        select
            sk_agencias
            , id_agencia
            , initcap(nome_agencia) as nome_agencia
            , initcap(cidade_agencia) as cidade_agencia
            , uf_agencia
            , case
                when uf_agencia in ('SP','RJ','ES','MG') then 'Sudeste'
                when uf_agencia in ('PR','SC','RS') then 'Sul'
                when uf_agencia in ('DF','GO','MT','MS') then 'Centro-Oeste'
                when uf_agencia in ('BA','SE','AL','PE','PB','RN','CE','PI','MA') then 'Nordeste'
                when uf_agencia in ('AM','PA','RO','RR','AC','AP','TO') then 'Norte'
              end as regiao_agencia
            , data_abertura_agencia
            , date_diff(current_date, data_abertura_agencia, year) as anos_atividade_agencia
            , tipo_agencia
        from surrogate_key
    )

select *
from enriched