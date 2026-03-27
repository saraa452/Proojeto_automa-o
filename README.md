# Portfolio Executivo de Relatorios Financeiros

Projeto de portfolio em Python para automatizar tratamento de lancamentos financeiros, gerar relatorios gerenciais e publicar um dashboard web interativo.

## O que o projeto entrega

- Pipeline de limpeza e padronizacao dos lancamentos
- Relatorios em CSV, Excel e Markdown
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
- relatorios/relatorio_gerencial_padronizado.xlsx
- docs/index.html
- docs/assets/dashboard-data.json

## Como executar

1. Ative o ambiente virtual.
2. Gere os relatorios e o site:

```bash
.venv/bin/python app.py
```

3. Para visualizar localmente com o JSON externo, rode um servidor HTTP na raiz do projeto:

```bash
python -m http.server 8000
```

4. Abra no navegador:

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

https://saraa452.github.io/Proojeto_automa-o/
