with
    int_contas as (
        select *
        from {{ ref('int_contas') }}
    )

    , final as (
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
            , dias_desde_abertura_conta
        from int_contas
    )

select *
from final