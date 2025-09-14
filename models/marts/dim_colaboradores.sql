with
    int_colaboradores as (
        select *
        from {{ ref('int_colaboradores') }}
    )

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
        from int_colaboradores
    )

select *
from final