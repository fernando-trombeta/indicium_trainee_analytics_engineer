with
    int_propostas_credito as (
        select *
        from {{ ref('int_propostas_credito') }}
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

    , dim_clientes as (
        select
            sk_clientes
            , id_cliente
        from {{ ref('dim_clientes') }}
    )

    , dim_colaboradores as (
        select
            sk_colaboradores
            , id_colaborador
        from {{ ref('dim_colaboradores') }}
    )

    , dim_colaborador_agencia as (
        select
            sk_colaborador_agencia
            , id_colaborador
            , id_agencia
        from {{ ref('dim_colaborador_agencia') }}
    )

    , dim_agencias as (
        select
            sk_agencias
            , id_agencia
        from {{ ref('dim_agencias') }}
    )

    , propostas_credito as (
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
        from {{ ref('int_propostas_credito') }}
    )

    , joined as (
        select
            propostas_credito.sk_propostas_credito
            , propostas_credito.id_proposta
            , propostas_credito.id_cliente
            , propostas_credito.id_colaborador
            , propostas_credito.data_entrada_proposta
            , propostas_credito.status_proposta
            , propostas_credito.taxa_juros_mensal
            , propostas_credito.valor_proposta
            , propostas_credito.valor_financiamento
            , propostas_credito.valor_entrada
            , propostas_credito.valor_prestacao
            , propostas_credito.quantidade_parcelas
            , propostas_credito.carencia

            , dim_clientes.sk_clientes
            , dim_colaboradores.sk_colaboradores

            , dim_colaborador_agencia.id_agencia
            , dim_agencias.sk_agencias
        from propostas_credito
        left join dim_clientes
          on propostas_credito.id_cliente = dim_clientes.id_cliente
        left join dim_colaboradores
          on propostas_credito.id_colaborador = dim_colaboradores.id_colaborador
        left join dim_colaborador_agencia
          on propostas_credito.id_colaborador = dim_colaborador_agencia.id_colaborador
        left join dim_agencias
          on dim_colaborador_agencia.id_agencia = dim_agencias.id_agencia
    )

    /* Enriquecer com calendário */
    , final as (
        select
            joined.sk_propostas_credito
            , joined.id_proposta
            , joined.id_cliente
            , joined.id_colaborador
            , joined.id_agencia

            , joined.sk_clientes
            , joined.sk_colaboradores
            , joined.sk_agencias

            , joined.data_entrada_proposta as data
            , dim_dates.ano
            , dim_dates.mes
            , dim_dates.trimestre
            , dim_dates.dia
            , dim_dates.dia_semana_num
            , dim_dates.dia_semana_nome
            , dim_dates.tipo_dia
            , dim_dates.ano_mes

            , joined.status_proposta
            , joined.taxa_juros_mensal
            , joined.valor_proposta
            , joined.valor_financiamento
            , joined.valor_entrada
            , joined.valor_prestacao
            , joined.quantidade_parcelas
            , joined.carencia

            -- medidas derivadas
            , case 
                when upper(joined.status_proposta) in ('APROVADA','APROVADO') then 1 
                else 0 
            end as flag_aprovada
        from joined
        left join dim_dates
          on joined.data_entrada_proposta = dim_dates.data
    )

select *
from final