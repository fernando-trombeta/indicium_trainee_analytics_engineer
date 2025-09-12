with 
    source_data as (
        select *
        from {{ ref('transacoes') }}
)      

    , transformed_data as (
        select
            cast(cod_transacao as int) as id_transacao
            , cast(num_conta as int) as numero_conta
            , safe_cast(substr(cast(data_transacao as string), 1, 10) as date) as data_transacao
            , cast(nome_transacao as string) as nome_transacao
            , cast(valor_transacao as numeric) as valor_transacao
        from source_data
)

select *
from transformed_data
