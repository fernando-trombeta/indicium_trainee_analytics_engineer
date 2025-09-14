# Banco Vitória: dbt + BigQuery + Looker Studio

## 📌 Visão Geral
Este projeto implementa um **Data Warehouse** para o Banco Vitória utilizando:
- **BigQuery** para armazenamento de dados
- **dbt (Data Build Tool)** para transformação e modelagem de dados
- **GitHub** Versionamento de Código
- **Looker Studio** para visualização e BI

Os dados foram fornecidos em **arquivos CSV** (clientes, contas, agências, colaboradores, propostas de crédito e transações) e processados em camadas de staging, intermediate e marts (dimensões e fatos).

## 🛠️ Processos
- Padronização de tipos e normalização de dados  
- Deduplicação de registros  
- Criação de **surrogate keys**  
- Enriquecimento de atributos (ex.: região da agência, ticket médio, flag de aprovação)  
- Modelagem dimensional (**star schema**) com tabelas de fatos e dimensões  

## 📊 Data Products
- **Dimensões**: clientes, contas, agências, colaboradores, calendário  
- **Fatos**: transações, propostas de crédito  
- **Dashboard no Looker Studio** para análises de negócio  

## 🚀 Stack Utilizada
- **BigQuery**  
- **dbt Core**
- **GitHub**
- **Google Looker Studio**
