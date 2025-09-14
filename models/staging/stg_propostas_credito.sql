with 
    source_data as (
        select *
        from {{ ref('propostas_credito') }}
)      

    , transformed_data as (
        select
            cast(cod_proposta as int) as id_proposta
            , cast(cod_cliente as int) as id_cliente
            , cast(cod_colaborador as int) as id_colaborador
            , safe_cast(substr(cast(data_entrada_proposta as string), 1, 10) as date) as data_entrada_proposta
            , cast(taxa_juros_mensal as numeric) as taxa_juros_mensal
            , cast(valor_proposta as numeric) as valor_proposta
            , cast(valor_financiamento as numeric) as valor_financiamento
            , cast(valor_entrada as numeric) as valor_entrada
            , cast(valor_prestacao as numeric) as valor_prestacao
            , cast(quantidade_parcelas as int) as quantidade_parcelas
            , cast(carencia as int) as carencia
            , cast(status_proposta as string) as status_proposta
        from source_data
)

select *
from transformed_data
