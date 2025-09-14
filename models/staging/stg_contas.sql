with 
    source_data as (
        select *
        from {{ ref('contas') }}
)      

    , transformed_data as (
        select
            cast(num_conta as int) as numero_conta
            , cast(cod_cliente as int) as id_cliente
            , cast(cod_agencia as int) as id_agencia
            , cast(cod_colaborador as int) as id_colaborador
            , upper(cast(tipo_conta as string)) as tipo_conta
            , safe_cast(substr(cast(data_abertura as string), 1, 10) as date) as data_abertura_conta
            , cast(saldo_total as numeric) as saldo_total_conta
            , cast(saldo_disponivel as numeric) as saldo_disponivel_conta
            , safe_cast(substr(cast(data_ultimo_lancamento as string), 1, 10) as date) as data_ultimo_lancamento_conta
        from source_data
)

select *
from transformed_data