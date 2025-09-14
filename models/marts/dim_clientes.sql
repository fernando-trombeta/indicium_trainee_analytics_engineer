with
    int_clientes as (
        select *
        from {{ ref('int_clientes') }}
    )

    , final as (
        select
            sk_clientes
            , id_cliente
            , primeiro_nome_cliente
            , ultimo_nome_cliente
            , nome_completo_cliente
            , email_cliente
            , tipo_cliente
            , data_inclusao_cliente
            , cpf_cnpj_cliente
            , data_nascimento_cliente
            , endereco_cliente
            , cep_cliente_num
            , cep_cliente
        from int_clientes
    )

select *
from final