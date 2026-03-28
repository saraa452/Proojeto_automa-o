# Portfolio Executivo de Relatorios Financeiros

Projeto de portfolio em Python para automatizar tratamento de lancamentos financeiros, gerar relatorios gerenciais e publicar um dashboard web interativo.

## O que o projeto entrega

- Pipeline de limpeza e padronizacao dos lancamentos
- Relatorios em CSV, Excel e Markdown
- Analises avancadas: sazonalidade e projecao de receitas/despesas
- Analises avancadas: eficiencia de descontos e payback por categoria
- Analises avancadas: rentabilidade com alocacao de custos compartilhados
- Analises avancadas: mix de receita recorrente vs projetos
- Analises avancadas: produtividade (pessoal x projetos)
- Analises avancadas: deteccao de anomalias e benchmarking YoY
- Analises avancadas: liquidez e fluxo de caixa com datas de recebimento/pagamento estimadas
- Dashboard web em HTML com:
- filtros por mes, categoria e centro de custo
- troca de etapa de analise (visao geral, mensal, categorias, centros e eficiencia)
- troca de indice por etapa
- grafico principal em canvas e tendencia complementar em SVG
- Dados do dashboard em arquivo externo JSON para facilitar manutencao

## Arquivos gerados

- relatorios/01_base_padronizada.csv
- relatorios/02_resumo_mensal.csv
- relatorios/03_resumo_categoria.csv
- relatorios/04_indicadores_eficiencia.csv
- relatorios/05_relatorio_executivo.md
- relatorios/06_projecao_mensal.csv
- relatorios/07_descontos_eficiencia.csv
- relatorios/07b_payback_descontos_categoria.csv
- relatorios/08_rentabilidade_centro_custo.csv
- relatorios/09_mix_receita.csv
- relatorios/10_produtividade_pessoal_projetos.csv
- relatorios/11_anomalias.csv
- relatorios/12_benchmarking_yoy.csv
- relatorios/13_fluxo_caixa_liquidez.csv
- relatorios/14_base_fluxo_com_datas.csv
- relatorios/15_sazonalidade_mensal.csv
- relatorios/relatorio_gerencial_padronizado.xlsx
- docs/index.html
- docs/assets/dashboard-data.json

## Como executar

1. Ative o ambiente virtual.
1. Gere os relatorios e o site:

```bash
.venv/bin/python app.py
```

1. Para visualizar localmente com o JSON externo, rode um servidor HTTP na raiz do projeto:

```bash
python -m http.server 8000
```

1. Abra no navegador:

```text
http://localhost:8000/docs/index.html
```

Observacao: abrir o HTML direto por arquivo (file://) pode bloquear o fetch do JSON em alguns navegadores.

## Gerar base de exemplo

Se o arquivo de entrada nao existir, voce pode criar uma base inicial automaticamente:

```bash
.venv/bin/python app.py --gerar-exemplo
```

## Argumentos uteis

```bash
.venv/bin/python app.py \
   --entrada dados/lancamentos_financeiros.csv \
   --saida relatorios \
   --site-dir docs \
   --titulo-relatorio "Portfolio Executivo - Sara" \
   --nome-profissional "Sara" \
   --cargo-profissional "Analista de Automacao Financeira" \
   --empresa "Analise Criterio" \
   --logo "assets/logo.svg"
```

## Publicar no GitHub Pages

1. Envie a branch main com a pasta docs atualizada.
2. No GitHub, abra Settings > Pages.
3. Em Build and deployment, selecione Deploy from a branch.
4. Branch: main.
5. Folder: /docs.
6. Salve.

URL esperada:

<https://saraa452.github.io/Proojeto_automa-o/>
