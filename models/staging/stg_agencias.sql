with 
    source_data as (
        select *
        from {{ ref('agencias') }}
)      

    , transformed_data as (
        select
            cast(cod_agencia as int) as id_agencia
            , cast(nome as string) as nome_agencia
            , cast(endereco as string) as endereco_agencia
            , cast(cidade as string) as cidade_agencia
            , upper(cast(uf as string)) as uf_agencia
            , cast(data_abertura as date) as data_abertura_agencia
            , cast(tipo_agencia as string) as tipo_agencia
        from source_data
)

select *
from transformed_data