with
    seed_dim_dates as (
        select *
        from {{ ref('dim_dates_2010_2025') }}
    )

    , transformed as (
        select
            cast(data as date) as data
            , cast(ano as int) as ano
            , cast(trimestre as int) as trimestre
            , cast(mes as int) as mes
            , cast(nome_mes as string) as nome_mes
            , cast(semana as int) as semana
            , cast(dia as int) as dia
            , cast(dia_semana_num as int) as dia_semana_num
            , cast(dia_semana_nome as string) as dia_semana_nome
            , cast(tipo_dia as string) as tipo_dia
            , cast(ano_mes as string) as ano_mes
            , cast(ano_mes_label as string) as ano_mes_label
            , cast(semestre as int) as semestre
            , cast(ano_semestre as string) as ano_semestre
            , cast(bimestre as int) as bimestre
            , cast(ano_bimestre as string) as ano_bimestre
            , cast(ano_trimestre as string) as ano_trimestre
            , cast(primeiro_dia_mes as date) as primeiro_dia_mes
            , cast(ultimo_dia_mes as date) as ultimo_dia_mes
            , cast(primeiro_dia_semana as date) as primeiro_dia_semana
            , cast(ultimo_dia_semana as date) as ultimo_dia_semana
            , cast(is_inicio_mes as bool) as is_inicio_mes
            , cast(is_fim_mes as bool) as is_fim_mes
        from seed_dim_dates
)

select *
from transformed