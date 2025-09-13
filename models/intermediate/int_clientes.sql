with
    stg_clientes as (
        select *
        from {{ ref('stg_clientes') }}
    )

    /* Remove possíveis duplicados */
    , dedup as (
        select
            *
            , row_number() over (
                partition by id_cliente
                order by data_inclusao_cliente desc nulls last
            ) as rn
        from stg_clientes
    )

    /* Gera a surrogate key */
    , surrogate_key as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_cliente', 'cpf_cnpj_cliente']) }} as sk_clientes
            , *
        from dedup
        where rn = 1
    )

    /* Enriquece os dados */
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
        from surrogate_key
    )

select *
from final