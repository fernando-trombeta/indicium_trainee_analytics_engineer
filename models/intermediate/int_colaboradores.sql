with
    stg_colaboradores as (
        select *
        from {{ ref('stg_colaboradores') }}
    )

    /* Remove possíveis duplicados */
    , dedup as (
        select
            *
            , row_number() over (
                partition by id_colaborador
                order by data_nascimento_colaborador desc
            ) as rn
        from stg_colaboradores
    )

    /* Gera a surrogate key */
    , surrogate_key as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_colaborador']) }} as sk_colaboradores
            , *
        from dedup
        where rn = 1
    )

    /* Enriquece os dados */
    , final as (
        select
            sk_colaboradores
            , id_colaborador
            , primeiro_nome_colaborador
            , ultimo_nome_colaborador
            , nome_completo_colaborador
            , email_colaborador
            , cpf_colaborador
            , data_nascimento_colaborador
            , endereco_colaborador
            , cep_colaborador_num
            , cep_colaborador
        from surrogate_key
    )

select *
from final