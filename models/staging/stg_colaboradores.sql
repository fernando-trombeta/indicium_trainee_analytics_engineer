with 
    source_data as (
        select *
        from {{ ref('colaboradores') }}
)      

    , transformed_data as (
        select
            cast(cod_colaborador as int) as id_colaborador
            , cast(primeiro_nome as string) as primeiro_nome_colaborador
            , cast(ultimo_nome as string) as ultimo_nome_colaborador
            , concat(primeiro_nome, ' ', ultimo_nome) AS nome_completo_colaborador
            , lower(cast(email as string)) as email_colaborador
            , cast(cpf as string) as cpf_colaborador
            , cast(data_nascimento as date) as data_nascimento_colaborador
            , cast(endereco as string) as endereco_colaborador
            -- CEP numérico (para joins com faixas de CEP)
            , safe_cast(lpad(regexp_replace(cast(cep as string), r'[^0-9]', ''), 8, '0') as int64) as cep_colaborador_num
            -- CEP com hífen (XXXXX-XXX) para consumo/exibição
            , (
                substr(lpad(regexp_replace(cast(cep as string), r'[^0-9]', ''), 8, '0'), 1, 5)
                || '-' ||
                substr(lpad(regexp_replace(cast(cep as string), r'[^0-9]', ''), 8, '0'), 6, 3)
              ) as cep_colaborador
        from source_data
)

select *
from transformed_data