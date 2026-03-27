"""Automacao de Rotinas Financeiras e Geracao de Relatorios Gerenciais.

Este modulo processa lancamentos financeiros em CSV, normaliza e limpa os
dados, e gera relatorios consolidados em CSV, Excel, Markdown e HTML
(pronto para publicacao no GitHub Pages).

Uso tipico::

    python app.py --gerar-exemplo
    python app.py --entrada dados/lancamentos_financeiros.csv

"""

from __future__ import annotations

import argparse
import logging
import shutil
from datetime import date
from pathlib import Path

import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


COLUNAS_PADRAO: list[str] = [
    "data",
    "descricao",
    "categoria",
    "tipo",
    "valor",
    "centro_custo",
]

ALIAS_COLUNAS: dict[str, str] = {
    "data": "data",
    "dt": "data",
    "descricao": "descricao",
    "descrição": "descricao",
    "historico": "descricao",
    "histórico": "descricao",
    "categoria": "categoria",
    "tipo": "tipo",
    "natureza": "tipo",
    "valor": "valor",
    "vlr": "valor",
    "centro_custo": "centro_custo",
    "centro de custo": "centro_custo",
}

SINONIMOS_TIPO: dict[str, str] = {
    "receitas": "receita",
    "despesas": "despesa",
    "entrada": "receita",
    "saida": "despesa",
    "saída": "despesa",
}

TEMPO_MANUAL_MIN: float = 2.5
TEMPO_AUTO_MIN: float = 0.3


def formatar_moeda_brl(valor: float) -> str:
    """Formata um numero no padrao monetario brasileiro."""
    texto = f"{valor:,.2f}"
    texto = texto.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {texto}"


def parse_valor_monetario(valor: object) -> float | None:
    """Converte uma string monetaria para float."""
    texto = str(valor).strip().replace("R$", "").replace(" ", "")
    if not texto:
        return None

    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")

    try:
        return float(texto)
    except ValueError:
        return None


def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia colunas para o padrao canonico e valida obrigatorias."""
    rename_map: dict[str, str] = {}
    for col in df.columns:
        chave = str(col).strip().lower()
        if chave in ALIAS_COLUNAS:
            rename_map[col] = ALIAS_COLUNAS[chave]

    df = df.rename(columns=rename_map)

    colunas_obrigatorias = ["data", "descricao", "categoria", "tipo", "valor"]
    faltantes = [c for c in colunas_obrigatorias if c not in df.columns]
    if faltantes:
        raise ValueError(
            f"Arquivo de entrada sem colunas obrigatorias: {faltantes}. "
            f"Esperado ao menos: {', '.join(colunas_obrigatorias)}."
        )

    if "centro_custo" not in df.columns:
        df["centro_custo"] = "geral"

    return df[COLUNAS_PADRAO].copy()


def _converter_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """Converte colunas data e valor para os tipos corretos."""
    df["data"] = pd.to_datetime(df["data"], errors="coerce", dayfirst=True)
    df["valor"] = df["valor"].map(parse_valor_monetario)
    return df


def _limpar_texto(df: pd.DataFrame) -> pd.DataFrame:
    """Remove espacos extras das colunas textuais."""
    for col in ["descricao", "categoria", "tipo", "centro_custo"]:
        df[col] = df[col].astype(str).str.strip()
    return df


def _normalizar_tipo(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza os valores da coluna tipo."""
    df["tipo"] = df["tipo"].str.lower().replace(SINONIMOS_TIPO)
    return df


def _filtrar_e_enriquecer(df: pd.DataFrame) -> pd.DataFrame:
    """Remove registros invalidos e adiciona colunas derivadas."""
    df = df.dropna(subset=["data", "valor"]).copy()
    df = df[df["tipo"].isin(["receita", "despesa"])]
    df["ano_mes"] = df["data"].dt.to_period("M").astype(str)
    df["valor_assinado"] = df.apply(
        lambda row: row["valor"] if row["tipo"] == "receita" else -row["valor"],
        axis=1,
    )
    return df


def limpar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """Executa o pipeline de limpeza e enriquecimento dos dados."""
    df = _converter_tipos(df)
    df = _limpar_texto(df)
    df = _normalizar_tipo(df)
    df = _filtrar_e_enriquecer(df)
    return df.sort_values(["data", "descricao"]).reset_index(drop=True)


def gerar_resumo_mensal(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega lancamentos por mes, separando receitas e despesas."""
    mensal = (
        df.groupby(["ano_mes", "tipo"], as_index=False)["valor"]
        .sum()
        .pivot(index="ano_mes", columns="tipo", values="valor")
        .fillna(0)
        .reset_index()
    )

    if "receita" not in mensal.columns:
        mensal["receita"] = 0.0
    if "despesa" not in mensal.columns:
        mensal["despesa"] = 0.0

    mensal["saldo"] = mensal["receita"] - mensal["despesa"]
    mensal = mensal.rename(
        columns={"receita": "total_receita", "despesa": "total_despesa"}
    )
    return mensal.sort_values("ano_mes").reset_index(drop=True)


def gerar_resumo_categoria(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega lancamentos por mes, categoria e tipo."""
    resumo = (
        df.groupby(["ano_mes", "categoria", "tipo"], as_index=False)["valor"]
        .sum()
    )
    registros = sorted(
        list(resumo.itertuples(index=False, name=None)),
        key=lambda row: (str(row[0]), str(row[2]), -float(row[3])),
    )
    return pd.DataFrame(registros, columns=resumo.columns).reset_index(drop=True)


def gerar_indicadores(
    df: pd.DataFrame,
    tempo_manual_min: float = TEMPO_MANUAL_MIN,
    tempo_auto_min: float = TEMPO_AUTO_MIN,
) -> pd.DataFrame:
    """Calcula indicadores de eficiencia da automacao."""
    total = len(df)
    horas_manuais = (total * tempo_manual_min) / 60
    horas_automatizadas = (total * tempo_auto_min) / 60
    horas_economizadas = max(horas_manuais - horas_automatizadas, 0)
    reducao_percentual = (
        (horas_economizadas / horas_manuais * 100) if horas_manuais > 0 else 0.0
    )

    return pd.DataFrame(
        [
            {"indicador": "total_lancamentos", "valor": total},
            {"indicador": "horas_manuais_estimadas", "valor": round(horas_manuais, 2)},
            {"indicador": "horas_automatizadas_estimadas", "valor": round(horas_automatizadas, 2)},
            {"indicador": "horas_economizadas", "valor": round(horas_economizadas, 2)},
            {"indicador": "reducao_tempo_percentual", "valor": round(reducao_percentual, 2)},
        ]
    )


def _extrair_kpis(mensal: pd.DataFrame, indicadores: pd.DataFrame) -> dict[str, float]:
    """Extrai os KPIs principais em um dicionario."""
    mapa = dict(zip(indicadores["indicador"], indicadores["valor"]))
    return {
        "receita_total": float(mensal["total_receita"].sum()),
        "despesa_total": float(mensal["total_despesa"].sum()),
        "saldo_total": float(mensal["saldo"].sum()),
        "meses_positivos": int((mensal["saldo"] > 0).sum()),
        "meses_total": len(mensal),
        "reducao_tempo": float(mapa.get("reducao_tempo_percentual", 0.0)),
        "horas_economizadas": float(mapa.get("horas_economizadas", 0.0)),
    }


def _top_despesas_por_categoria(categoria: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    """Retorna as n categorias com maior volume de despesas."""
    top = (
        categoria[categoria["tipo"] == "despesa"]
        .groupby("categoria", as_index=False)["valor"]
        .sum()
    )
    registros = sorted(
        list(top.itertuples(index=False, name=None)),
        key=lambda row: float(row[1]),
        reverse=True,
    )[:n]
    return pd.DataFrame(registros, columns=top.columns).reset_index(drop=True)


def _preparar_logo_site(logo_path: str, pasta_site: Path) -> str:
    """Copia o logo para dentro do site publicado e retorna o caminho relativo."""
    origem = Path(logo_path)
    if not origem.is_absolute():
        origem = Path.cwd() / origem

    if not origem.exists():
        logger.warning("Logo nao encontrado em %s. O HTML usara o caminho informado.", origem)
        return logo_path

    pasta_assets = pasta_site / "assets"
    pasta_assets.mkdir(parents=True, exist_ok=True)
    destino = pasta_assets / origem.name
    shutil.copyfile(origem, destino)
    return f"assets/{origem.name}"


def gerar_relatorio_executivo_markdown(
    mensal: pd.DataFrame,
    categoria: pd.DataFrame,
    indicadores: pd.DataFrame,
    pasta_saida: Path,
    titulo: str,
    nome_profissional: str,
    cargo_profissional: str,
    empresa: str,
    logo_path: str,
) -> Path:
    """Gera o relatorio executivo em formato Markdown."""
    pasta_saida.mkdir(parents=True, exist_ok=True)
    arquivo_md = pasta_saida / "05_relatorio_executivo.md"

    kpis = _extrair_kpis(mensal, indicadores)
    top_desp = _top_despesas_por_categoria(categoria)
    hoje = date.today().strftime("%d/%m/%Y")

    linhas_despesas = [
        f"- {row['categoria']}: {formatar_moeda_brl(float(row['valor']))}"
        for _, row in top_desp.iterrows()
    ]

    linhas_mensal = [
        f"| {row['ano_mes']} | "
        f"{formatar_moeda_brl(float(row['total_receita']))} | "
        f"{formatar_moeda_brl(float(row['total_despesa']))} | "
        f"{formatar_moeda_brl(float(row['saldo']))} |"
        for _, row in mensal.iterrows()
    ]

    conteudo = [
        "<div align='center'>",
        f"  <img src='{logo_path}' alt='Logotipo' width='180'>",
        f"  <h1>{titulo}</h1>",
        "  <p><strong>Case de Portfolio | Automacao Financeira com foco em eficiencia operacional</strong></p>",
        f"  <p>{nome_profissional} | {cargo_profissional}</p>",
        f"  <p>{empresa}</p>",
        f"  <p>Data de emissao: {hoje}</p>",
        "</div>",
        "",
        "---",
        "",
        f"# {titulo}",
        "",
        "## Resumo Executivo",
        (
            "Projeto de portfolio desenvolvido para demonstrar capacidade de estruturar "
            "rotinas financeiras automatizadas, reduzir tempo operacional e transformar "
            "dados transacionais em relatorios gerenciais padronizados."
        ),
        "",
        "## 1. Objetivo",
        (
            "Demonstrar dominio em automacao de processos financeiros, consolidacao de "
            "dados e comunicacao executiva de indicadores para suporte a tomada de decisao."
        ),
        "",
        "## 2. Indicadores-Chave",
        f"- Receita total consolidada: {formatar_moeda_brl(kpis['receita_total'])}",
        f"- Despesa total consolidada: {formatar_moeda_brl(kpis['despesa_total'])}",
        f"- Saldo acumulado: {formatar_moeda_brl(kpis['saldo_total'])}",
        f"- Meses com saldo positivo: {kpis['meses_positivos']} de {kpis['meses_total']}",
        f"- Reducao estimada de tempo operacional: {kpis['reducao_tempo']:.2f}%",
        f"- Horas economizadas no periodo: {kpis['horas_economizadas']:.2f}h",
        "",
        "## 3. Painel Mensal Padronizado",
        "| Mes | Receita | Despesa | Saldo |",
        "|---|---:|---:|---:|",
        *linhas_mensal,
        "",
        "## 4. Top 5 Categorias de Despesa",
        *linhas_despesas,
        "",
        "## 5. Resultado do Projeto",
        (
            "A solucao reduz dependencia de consolidacoes manuais, aumenta a confiabilidade "
            "dos dados e acelera a geracao de visoes executivas para acompanhamento financeiro."
        ),
        "",
        "## 6. Competencias Demonstradas",
        "- Automacao de rotinas operacionais com Python",
        "- Padronizacao de relatorios gerenciais",
        "- Tratamento e validacao de dados financeiros",
        "- Geracao de entregaveis executivos em multiplos formatos",
        "- Preparacao de publicacao web para portfolio profissional",
        "",
        "## 7. Entregaveis",
        "- 01_base_padronizada.csv",
        "- 02_resumo_mensal.csv",
        "- 03_resumo_categoria.csv",
        "- 04_indicadores_eficiencia.csv",
        "- 05_relatorio_executivo.md",
        "- docs/index.html",
    ]

    arquivo_md.write_text("\n".join(conteudo), encoding="utf-8")
    logger.info("Relatorio Markdown gerado: %s", arquivo_md)
    return arquivo_md


def gerar_relatorio_executivo_html(
    mensal: pd.DataFrame,
    categoria: pd.DataFrame,
    indicadores: pd.DataFrame,
    pasta_site: Path,
    titulo: str,
    nome_profissional: str,
    cargo_profissional: str,
    empresa: str,
    logo_path: str,
) -> Path:
    """Gera o relatorio executivo em HTML para GitHub Pages."""
    pasta_site.mkdir(parents=True, exist_ok=True)
    arquivo_html = pasta_site / "index.html"

    kpis = _extrair_kpis(mensal, indicadores)
    top_desp = _top_despesas_por_categoria(categoria)
    hoje = date.today().strftime("%d/%m/%Y")
    logo_site = _preparar_logo_site(logo_path, pasta_site)

    linhas_mensal = "".join(
        "<tr>"
        f"<td>{row['ano_mes']}</td>"
        f"<td>{formatar_moeda_brl(float(row['total_receita']))}</td>"
        f"<td>{formatar_moeda_brl(float(row['total_despesa']))}</td>"
        f"<td>{formatar_moeda_brl(float(row['saldo']))}</td>"
        "</tr>"
        for _, row in mensal.iterrows()
    )

    linhas_despesa = "".join(
        f"<li><strong>{row['categoria']}</strong>: {formatar_moeda_brl(float(row['valor']))}</li>"
        for _, row in top_desp.iterrows()
    )

    classe_saldo = "saldo-ok" if kpis["saldo_total"] >= 0 else "saldo-alerta"

    html = f"""<!doctype html>
<html lang="pt-BR">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{titulo}</title>
    <style>
        :root {{
            --bg: #f4f8fb;
            --card: #ffffff;
            --ink: #102a43;
            --muted: #486581;
            --brand: #0b3c5d;
            --accent: #328cc1;
            --line: #d9e2ec;
            --ok: #166534;
            --warn: #9a3412;
        }}
        * {{ box-sizing: border-box; }}
        body {{
            margin: 0;
            font-family: "Segoe UI", "Trebuchet MS", sans-serif;
            color: var(--ink);
            background:
                radial-gradient(circle at 10% 10%, #d9ecf7 0%, transparent 35%),
                radial-gradient(circle at 90% 20%, #dceff9 0%, transparent 40%),
                var(--bg);
            line-height: 1.5;
        }}
        .container {{ max-width: 1040px; margin: 0 auto; padding: 24px 16px 48px; }}
        .hero {{
            background: linear-gradient(135deg, #0b3c5d, #328cc1);
            color: #fff;
            border-radius: 18px;
            padding: 28px;
            box-shadow: 0 14px 32px rgba(11, 60, 93, 0.2);
        }}
        .hero img {{ width: 120px; background: #fff; border-radius: 12px; padding: 8px; }}
        .hero h1 {{ margin: 14px 0 6px; font-size: clamp(1.5rem, 3.2vw, 2.3rem); }}
        .hero p {{ margin: 4px 0; opacity: 0.95; }}
        .grid {{
            display: grid;
            gap: 14px;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            margin-top: 18px;
        }}
        .kpi {{
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 14px;
            padding: 14px;
        }}
        .kpi small {{ color: var(--muted); display: block; }}
        .kpi strong {{ font-size: 1.1rem; }}
        .panel {{
            margin-top: 18px;
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 14px;
            padding: 16px;
        }}
        h2 {{ margin-top: 4px; color: var(--brand); }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ border-bottom: 1px solid var(--line); padding: 10px; text-align: left; }}
        th {{ background: #f0f4f8; }}
        ul {{ margin: 0; padding-left: 20px; }}
        .foot {{ margin-top: 18px; color: var(--muted); font-size: 0.92rem; }}
        .saldo-ok {{ color: var(--ok); }}
        .saldo-alerta {{ color: var(--warn); }}
    </style>
</head>
<body>
    <main class="container">
        <section class="hero">
            <img src="{logo_site}" alt="Logotipo">
            <h1>{titulo}</h1>
            <p><strong>Case de Portfolio | Automacao Financeira com foco em eficiencia operacional</strong></p>
            <p>{nome_profissional} | {cargo_profissional}</p>
            <p>{empresa}</p>
            <p>Data de emissao: {hoje}</p>
        </section>

        <section class="panel">
            <h2>Resumo Executivo</h2>
            <p>
                Projeto de portfolio desenvolvido para demonstrar capacidade de automatizar
                rotinas financeiras, reduzir esforco manual e transformar dados operacionais
                em relatorios executivos padronizados para acompanhamento gerencial.
            </p>
        </section>

        <section class="grid">
            <article class="kpi"><small>Receita Total</small><strong>{formatar_moeda_brl(kpis['receita_total'])}</strong></article>
            <article class="kpi"><small>Despesa Total</small><strong>{formatar_moeda_brl(kpis['despesa_total'])}</strong></article>
            <article class="kpi"><small>Saldo Acumulado</small><strong class="{classe_saldo}">{formatar_moeda_brl(kpis['saldo_total'])}</strong></article>
            <article class="kpi"><small>Meses Positivos</small><strong>{kpis['meses_positivos']}</strong></article>
            <article class="kpi"><small>Reducao de Tempo</small><strong>{kpis['reducao_tempo']:.2f}%</strong></article>
            <article class="kpi"><small>Horas Economizadas</small><strong>{kpis['horas_economizadas']:.2f}h</strong></article>
        </section>

        <section class="panel">
            <h2>Painel Mensal Padronizado</h2>
            <table>
                <thead>
                    <tr><th>Mes</th><th>Receita</th><th>Despesa</th><th>Saldo</th></tr>
                </thead>
                <tbody>
                    {linhas_mensal}
                </tbody>
            </table>
        </section>

        <section class="panel">
            <h2>Top 5 Categorias de Despesa</h2>
            <ul>{linhas_despesa}</ul>
        </section>

        <section class="panel">
            <h2>Resultado do Projeto</h2>
            <p>
                A solucao reduz dependencia de consolidacoes manuais, eleva a confiabilidade
                da informacao e acelera a entrega de indicadores para apoio a decisoes gerenciais.
            </p>
            <p>
                O projeto evidencia competencias praticas em Python, modelagem de dados,
                padronizacao de relatorios e construcao de entregaveis prontos para ambiente web.
            </p>
            <p class="foot">Publicacao web pronta para GitHub Pages a partir da pasta docs.</p>
        </section>
    </main>
</body>
</html>
"""

    arquivo_html.write_text(html, encoding="utf-8")
    logger.info("Relatorio HTML gerado: %s", arquivo_html)
    return arquivo_html


def salvar_relatorios(
    base: pd.DataFrame,
    mensal: pd.DataFrame,
    categoria: pd.DataFrame,
    indicadores: pd.DataFrame,
    pasta_saida: Path,
) -> bool:
    """Exporta DataFrames para CSV e, opcionalmente, para Excel."""
    pasta_saida.mkdir(parents=True, exist_ok=True)

    arquivos_csv = {
        "01_base_padronizada.csv": base,
        "02_resumo_mensal.csv": mensal,
        "03_resumo_categoria.csv": categoria,
        "04_indicadores_eficiencia.csv": indicadores,
    }
    for nome, df in arquivos_csv.items():
        df.to_csv(pasta_saida / nome, index=False)

    arquivo_excel = pasta_saida / "relatorio_gerencial_padronizado.xlsx"
    try:
        with pd.ExcelWriter(arquivo_excel) as writer:
            base.to_excel(writer, sheet_name="base_padronizada", index=False)
            mensal.to_excel(writer, sheet_name="resumo_mensal", index=False)
            categoria.to_excel(writer, sheet_name="resumo_categoria", index=False)
            indicadores.to_excel(writer, sheet_name="indicadores", index=False)
        logger.info("Arquivo Excel gerado: %s", arquivo_excel)
        return True
    except ModuleNotFoundError:
        logger.warning(
            "openpyxl nao instalado. Arquivo Excel nao foi gerado. "
            "Para habilitar, execute: pip install openpyxl"
        )
        return False


def imprimir_painel(mensal: pd.DataFrame, indicadores: pd.DataFrame) -> None:
    """Exibe um resumo do processamento no terminal."""
    separador = "=" * 72
    logger.info(separador)
    logger.info("AUTOMACAO DE RELATORIOS FINANCEIROS")
    logger.info(separador)
    logger.info("Resumo mensal:\n%s", mensal.to_string(index=False))
    logger.info("Indicadores de eficiencia:\n%s", indicadores.to_string(index=False))
    logger.info(separador)


def criar_exemplo(arquivo: Path) -> None:
    """Cria um arquivo CSV de exemplo com lancamentos ficticios."""
    exemplo = pd.DataFrame(
        [
            ["01/02/2026", "Venda projeto A", "vendas", "receita", 15000.00, "comercial"],
            ["03/02/2026", "Pagamento fornecedor X", "fornecedores", "despesa", 4200.50, "operacoes"],
            ["10/02/2026", "Assinatura software", "tecnologia", "despesa", 899.90, "ti"],
            ["15/02/2026", "Venda projeto B", "vendas", "receita", 8700.00, "comercial"],
            ["05/03/2026", "Folha salarial", "pessoal", "despesa", 6800.00, "rh"],
            ["14/03/2026", "Recebimento consultoria", "servicos", "receita", 4600.00, "consultoria"],
            ["20/03/2026", "Energia eletrica", "infraestrutura", "despesa", 740.31, "adm"],
        ],
        columns=COLUNAS_PADRAO,
    )
    arquivo.parent.mkdir(parents=True, exist_ok=True)
    exemplo.to_csv(arquivo, index=False)
    logger.info("Arquivo de exemplo criado em: %s", arquivo)


def parse_args() -> argparse.Namespace:
    """Analisa os argumentos da linha de comando."""
    parser = argparse.ArgumentParser(
        description="Automacao de rotinas financeiras e padronizacao de relatorios gerenciais.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--entrada",
        default="dados/lancamentos_financeiros.csv",
        help="Arquivo CSV com lancamentos financeiros.",
    )
    parser.add_argument(
        "--saida",
        default="relatorios",
        help="Diretorio de saida dos relatorios.",
    )
    parser.add_argument(
        "--gerar-exemplo",
        action="store_true",
        help="Gera um arquivo de exemplo caso ainda nao exista.",
    )
    parser.add_argument(
        "--titulo-relatorio",
        default="Portfolio Executivo - Sara",
        help="Titulo do relatorio executivo.",
    )
    parser.add_argument(
        "--nome-profissional",
        default="Sara",
        help="Nome para a capa do relatorio.",
    )
    parser.add_argument(
        "--cargo-profissional",
        default="Analista de Automacao Financeira",
        help="Cargo para a capa do relatorio.",
    )
    parser.add_argument(
        "--empresa",
        default="Analise Criterio",
        help="Empresa ou projeto para a capa do relatorio.",
    )
    parser.add_argument(
        "--logo",
        default="assets/logo.svg",
        help="Caminho do logotipo usado na capa.",
    )
    parser.add_argument(
        "--site-dir",
        default="docs",
        help="Diretorio para site HTML publicado no GitHub Pages.",
    )
    return parser.parse_args()


def main() -> None:
    """Ponto de entrada principal do script."""
    args = parse_args()
    arquivo_entrada = Path(args.entrada)
    pasta_saida = Path(args.saida)
    pasta_site = Path(args.site_dir)

    if args.gerar_exemplo and not arquivo_entrada.exists():
        criar_exemplo(arquivo_entrada)

    if not arquivo_entrada.exists():
        raise FileNotFoundError(
            f"Arquivo de entrada nao encontrado: {arquivo_entrada}. "
            "Use --gerar-exemplo para criar uma base inicial."
        )

    logger.info("Lendo arquivo de entrada: %s", arquivo_entrada)
    bruto = pd.read_csv(arquivo_entrada)
    base = limpar_dados(normalizar_colunas(bruto))

    if base.empty:
        raise ValueError("Nenhum lancamento valido encontrado apos limpeza dos dados.")

    resumo_mensal = gerar_resumo_mensal(base)
    resumo_categoria = gerar_resumo_categoria(base)
    indicadores = gerar_indicadores(base)

    excel_gerado = salvar_relatorios(
        base,
        resumo_mensal,
        resumo_categoria,
        indicadores,
        pasta_saida,
    )

    arquivo_md = gerar_relatorio_executivo_markdown(
        resumo_mensal,
        resumo_categoria,
        indicadores,
        pasta_saida,
        args.titulo_relatorio,
        args.nome_profissional,
        args.cargo_profissional,
        args.empresa,
        args.logo,
    )
    arquivo_html = gerar_relatorio_executivo_html(
        resumo_mensal,
        resumo_categoria,
        indicadores,
        pasta_site,
        args.titulo_relatorio,
        args.nome_profissional,
        args.cargo_profissional,
        args.empresa,
        args.logo,
    )

    imprimir_painel(resumo_mensal, indicadores)

    logger.info("Relatorios gerados com sucesso:")
    for arq in [
        pasta_saida / "01_base_padronizada.csv",
        pasta_saida / "02_resumo_mensal.csv",
        pasta_saida / "03_resumo_categoria.csv",
        pasta_saida / "04_indicadores_eficiencia.csv",
        arquivo_md,
        arquivo_html,
    ]:
        logger.info("  - %s", arq)

    if excel_gerado:
        logger.info("  - %s", pasta_saida / "relatorio_gerencial_padronizado.xlsx")


if __name__ == "__main__":
    main()