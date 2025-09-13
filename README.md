# One Data

O repositório "One Data" é uma solução para o programa Data Master organizado pela F1rst Santander. Solução proposta e desenvolvida por [Vinicius Otoni](http://linkedin.com/in/vinicius-otoni-b330b3295/)

## Conteúdos do Repositório

1. [Visão Inicial](#1-visão-inicial)  
   - [Objetivo](#11-objetivo)  
   - [Visão do fluxo](#12-visão-do-fluxo)  
   - [Componentes](#13-componentes)  


## 1. Visão Inicial

Sessão dedicada ao esclarecimento do propósito do projeto e ao melhor entendimento da sua proposta de valor.

### 1.1 Objetivo

One Data é uma solução com a finalidade de **DEMOCRATIZAR** o acesso inicial ao lakehouse, atuando a princípio em duas camadas fundamentais:

- Bronze → responsável pelo processo de ingestão dos dados.

- Silver → camada que garante dados mais confiáveis, já tratados e estruturados.

Mas afinal, o que significa democratizar nesse contexto?

##### Problemática: 

Atualmente, existe uma forte dependência de conhecimento técnico avançado para realizar ingestão de dados em um lakehouse — e ainda maior para elevá-los às camadas de maior valor, como Silver e Gold. Além disso, muitos processos são executados de forma desorganizada, sem padronização e com baixa governança, o que compromete a confiabilidade e a rastreabilidade do ecossistema.

É nesse cenário que surge o conceito de **DEMOCRATIZAÇÃO**: possibilitar que qualquer usuário, mesmo sem expertise técnica, consiga ingerir e higienizar seus dados de maneira automática, padronizada e governada, seguindo boas práticas de qualidade e evitando redundância de processos.

Assim, o One Data não apenas simplifica a interação com o lakehouse, mas também garante consistência, segurança e escalabilidade, assegurando que todos os dados atendam ao mesmo padrão de excelência.

### 1.2 Ideação do Projeto

O projeto foi idealizado para eliminar a dependência técnica do usuário, oferecendo uma aplicação segura, governada e totalmente automatizada. Dessa forma, qualquer interação com os dados ocorre dentro de um fluxo padronizado e confiável, garantindo qualidade e rastreabilidade ponta a ponta.

Sendo assim, o processo seguirá o seguinte fluxo de trabalho:

<p align="left">
  <img src="./assets/images/fluxo-ideacao-data.png" alt="Ideação Projeto" width="100%" style="border-radius: 1%;"/>
</p>

## 2. Arquitetura Técnica

O fluxo do One Data inicia-se com o preenchimento do [Data Contract](#4-data-contract), documento responsável por descrever a estrutura e as regras esperadas do dataset. A partir desse contrato, a ingestão é realizada automaticamente por meio do Auto Loader, que garante escalabilidade e eficiência na chegada dos dados.

Em seguida, os dados passam por uma Engine interna, apoiada pelo framework [DQX](#6-dqx-framework), responsável por aplicar health checks e validações de qualidade, assegurando consistência, confiabilidade e aderência às regras de governança definidas.

A arquitetura está organizada em duas camadas principais:

- Bronze
   Camada destinada à ingestão bruta dos dados, preservando o formato original e mantendo rastreabilidade completa da origem.

<p align="left"> <img src="./assets/images/fluxo-ingestao.png" alt="Arquitetura Bronze" width="100%" style="border-radius: 1%;"/> </p>

- Silver
   Camada responsável pela padronização e enriquecimento dos dados, aplicando as regras do Data Contract e validações do DQX. Aqui os dados tornam-se confiáveis e prontos para análises mais avançadas ou consumo por áreas de negócio.

<p align="left"> <img src="./assets/images/fluxo-silver.png" alt="Arquitetura Silver" width="100%" style="border-radius: 1%;"/> </p>

## 3. Medalium Architecture

A [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture) é um padrão recomendado pelo Databricks para organizar dados em camadas lógicas dentro do lakehouse. O objetivo é simplificar o ciclo de vida dos dados, garantindo qualidade, governança, escalabilidade e reuso ao longo das transformações.

No contexto do One Data, a arquitetura é aplicada da seguinte forma:

- **Bronze**

   - Armazena os dados brutos, ingeridos diretamente da fonte.

   - Mantém o histórico completo e preserva o formato original.

   - Permite auditoria e reprocessamento, caso ajustes sejam necessários.

   - Principal responsabilidade: rastreabilidade.

- **Silver**

   - Contém dados padronizados, limpos e validados.

   - Aqui são aplicados os contratos de dados, transformações de schema e as regras do framework de qualidade (DQX).

   - Representa uma camada de confiabilidade, garantindo que os dados estejam prontos para análises consistentes.

   - Principal responsabilidade: consistência e confiabilidade.


<p align="left"> <img src="./assets/images/medallion-architecture.png" alt="Arquitetura Medallion" width="100%" style="border-radius: 1%;"/> </p>

## 4. Data Contract



## 5. Auto Loader & Ingestão

## 6. DQX Framework

## 7. ETL Engine

## 8. GitHub Actions

## 9. Melhorias Futuras

## 10. Referências
