with 
    source_data as (
        select *
        from {{ ref('clientes') }}
)      

    , transformed_data as (
        select
            cast(cod_cliente as int) as id_cliente
            , cast(primeiro_nome as string) as primeiro_nome_cliente
            , cast(ultimo_nome as string) as ultimo_nome_cliente
            , concat(primeiro_nome, ' ', ultimo_nome) as nome_completo_cliente
            , cast(email as string) as email_cliente
            , upper(cast(tipo_cliente as string)) as tipo_cliente
            , safe_cast(substr(cast(data_inclusao as string), 1, 10) as date) as data_inclusao_cliente
            , cast(cpfcnpj as string) as cpf_cnpj_cliente
            , cast(data_nascimento as date) as data_nascimento_cliente
            , cast(endereco as string) as endereco_cliente
            , cast(cep as string) as cep_cliente
        from source_data
)

select *
from transformed_data