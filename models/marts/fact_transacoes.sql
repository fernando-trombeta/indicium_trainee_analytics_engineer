with
    int_transacoes as (
        select *
        from {{ ref('int_transacoes') }}
    )

    , dim_contas as (
        select
            sk_contas
            , numero_conta
            , id_cliente
            , id_agencia
            , id_colaborador
        from {{ ref('dim_contas') }}
    )

    , dim_clientes as (
        select
            sk_clientes
            , id_cliente
        from {{ ref('dim_clientes') }}
    )

    , dim_agencias as (
        select
            sk_agencias
            , id_agencia
        from {{ ref('dim_agencias') }}
    )

    , dim_colaboradores as (
        select
            sk_colaboradores
            , id_colaborador
        from {{ ref('dim_colaboradores') }}
    )

    , dim_dates as (
        select
            data
            , ano
            , mes
            , trimestre
            , dia
            , dia_semana_num
            , dia_semana_nome
            , tipo_dia
            , ano_mes
        from {{ ref('dim_dates') }}
    )

    , joined as (
        select
            int_transacoes.sk_transacao
            , int_transacoes.id_transacao
            , int_transacoes.numero_conta
            , int_transacoes.data_transacao
            , int_transacoes.nome_transacao
            , int_transacoes.valor_transacao

            , dim_contas.id_cliente
            , dim_contas.id_agencia
            , dim_contas.id_colaborador

            , dim_contas.sk_contas
            , dim_clientes.sk_clientes
            , dim_agencias.sk_agencias
            , dim_colaboradores.sk_colaboradores
        from int_transacoes
        left join dim_contas
          on int_transacoes.numero_conta = dim_contas.numero_conta
        left join dim_clientes
          on dim_contas.id_cliente = dim_clientes.id_cliente
        left join dim_agencias
          on dim_contas.id_agencia = dim_agencias.id_agencia
        left join dim_colaboradores
          on dim_contas.id_colaborador = dim_colaboradores.id_colaborador
    )

    /* Enriquecer com atributos de calendário e calcula alguns auxiliares */
    , final as (
        select
            joined.sk_transacao
            , joined.id_transacao

            -- FKs naturais e substitutas (para navegação no BI)
            , joined.numero_conta
            , joined.id_cliente
            , joined.id_agencia
            , joined.id_colaborador
            , joined.sk_contas
            , joined.sk_clientes
            , joined.sk_agencias
            , joined.sk_colaboradores

            , joined.data_transacao as data
            , dim_dates.ano
            , dim_dates.mes
            , dim_dates.trimestre
            , dim_dates.dia
            , dim_dates.dia_semana_num
            , dim_dates.dia_semana_nome
            , dim_dates.tipo_dia
            , dim_dates.ano_mes

            , joined.valor_transacao

            , joined.nome_transacao
            , case 
                when joined.valor_transacao >= 0 then 1 
                else 0 
            end as flag_credito
            , case 
                when joined.valor_transacao  < 0 then 1 
                else 0 
            end as flag_debito
        from joined
        left join dim_dates
          on joined.data_transacao = dim_dates.data
    )

select *
from final