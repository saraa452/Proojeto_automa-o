# Portfolio Executivo de Relatorios Financeiros

Case de portfolio em Python para demonstrar automacao de rotinas operacionais financeiras, reducao de tempo manual e padronizacao de relatorios gerenciais com entrega executiva pronta para web.

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

## Competencias demonstradas

- Automacao de processos financeiros com Python
- Tratamento, limpeza e padronizacao de dados
- Geracao de relatorios em CSV, Excel, Markdown e HTML
- Comunicacao executiva orientada a indicadores
- Publicacao de portfolio tecnico no GitHub Pages

## Publicar no GitHub Pages (Linux)

1. Configure sua identidade Git:

```bash
git config --global user.name "Sara"
git config --global user.email "seu-email@exemplo.com"
```

1. Inicialize o Git, adicione os arquivos e faça commit:

```bash
git init
git add .
git commit -m "Portfolio executivo de relatorios financeiros"
git branch -M main
```

1. Crie o repositório no GitHub e conecte remoto:

```bash
git remote add origin "https://github.com/SEU_USUARIO/SEU_REPOSITORIO.git"
git push -u origin main
```

1. No GitHub:
   - Abra Settings > Pages
   - Em Build and deployment, selecione Deploy from a branch
   - Branch: main
   - Folder: /docs
   - Salve

1. A URL final fica assim:

```text
https://SEU_USUARIO.github.io/SEU_REPOSITORIO/
```

## Personalizacao

Voce pode ajustar titulo, nome, cargo, empresa e logo com argumentos opcionais:

/home/sara/Documentos/Projetos_gits/Analise_critério/.venv/bin/python app.py --titulo-relatorio "Portfolio Executivo - Sara" --nome-profissional "Sara" --cargo-profissional "Analista de Automacao Financeira" --empresa "Analise Criterio" --logo "../assets/logo.svg"
