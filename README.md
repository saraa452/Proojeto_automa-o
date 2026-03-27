# Portfolio Executivo de Relatorios Financeiros

Projeto em Python para automacao de rotinas operacionais, reducao de tempo manual e padronizacao de relatorios gerenciais.

## Entregaveis gerados

- relatorios/01_base_padronizada.csv
- relatorios/02_resumo_mensal.csv
- relatorios/03_resumo_categoria.csv
- relatorios/04_indicadores_eficiencia.csv
- relatorios/05_relatorio_executivo.md
- relatorios/relatorio_gerencial_padronizado.xlsx
- docs/index.html (versao web para GitHub Pages)

## Como gerar os relatorios

1. Ative seu ambiente virtual.
2. Execute o script:

   /home/sara/Documentos/Projetos_gits/Analise_critério/.venv/bin/python app.py --gerar-exemplo

3. Abra a versao web em docs/index.html.

## Publicar no GitHub Pages (Linux)

1. Inicialize o Git e faça commit:

   git init
   git add .
   git commit -m "Portfolio executivo de relatorios financeiros"

2. Crie o repositório no GitHub e conecte remoto:

   git remote add origin https://github.com/saraa452/Proojeto_automa-o.git
   git branch -M main
   git push -u origin main

3. No GitHub:
   - Abra Settings > Pages
   - Em Build and deployment, selecione Deploy from a branch
   - Branch: main
   - Folder: /docs
   - Salve

4. A URL final fica assim:

   https://saraa452.github.io/Proojeto_automa-o/

## Personalizacao

Voce pode ajustar titulo, nome, cargo, empresa e logo com argumentos opcionais:

/home/sara/Documentos/Projetos_gits/Analise_critério/.venv/bin/python app.py --titulo-relatorio "Portfolio Executivo - Sara" --nome-profissional "Sara" --cargo-profissional "Analista de Automacao Financeira" --empresa "Analise Criterio" --logo "../assets/logo.svg"
