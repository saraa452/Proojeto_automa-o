from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd


COLUNAS_PADRAO = [
    "data",
    "descricao",
    "categoria",
    "tipo",
    "valor",
    "centro_custo",
]

ALIAS_COLUNAS = {
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


def formatar_moeda_brl(valor: float) -> str:
    texto = f"{valor:,.2f}"
    texto = texto.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {texto}"


def parse_valor_monetario(valor: object) -> float | None:
    texto = str(valor).strip().replace("R$", "").replace(" ", "")
    if not texto:
        return None

    # Se possui virgula, assume formato brasileiro: 1.234,56
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")

    try:
        return float(texto)
    except ValueError:
        return None


def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for col in df.columns:
        chave = str(col).strip().lower()
        if chave in ALIAS_COLUNAS:
            rename_map[col] = ALIAS_COLUNAS[chave]

    df = df.rename(columns=rename_map)

    faltantes = [col for col in ["data", "descricao", "categoria", "tipo", "valor"] if col not in df.columns]
    if faltantes:
        raise ValueError(
            f"Arquivo de entrada sem colunas obrigatorias: {faltantes}. "
            f"Esperado ao menos: data, descricao, categoria, tipo, valor."
        )

    if "centro_custo" not in df.columns:
        df["centro_custo"] = "geral"

    return df[COLUNAS_PADRAO].copy()


def limpar_dados(df: pd.DataFrame) -> pd.DataFrame:
    # Ajusta formatos brasileiros e remove registros incompletos.
    df["data"] = pd.to_datetime(df["data"], errors="coerce", dayfirst=True)
    df["valor"] = df["valor"].map(parse_valor_monetario)

    for col in ["descricao", "categoria", "tipo", "centro_custo"]:
        df[col] = df[col].astype(str).str.strip()

    df["tipo"] = df["tipo"].str.lower().replace(
        {
            "receitas": "receita",
            "despesas": "despesa",
            "entrada": "receita",
            "saida": "despesa",
            "saída": "despesa",
        }
    )

    validos = df.dropna(subset=["data", "valor"]).copy()
    validos = validos[validos["tipo"].isin(["receita", "despesa"])]
    validos["ano_mes"] = validos["data"].dt.to_period("M").astype(str)
    validos["valor_assinado"] = validos.apply(
        lambda row: row["valor"] if row["tipo"] == "receita" else -row["valor"], axis=1
    )
    return validos.sort_values(["data", "descricao"]).reset_index(drop=True)


def gerar_resumo_mensal(df: pd.DataFrame) -> pd.DataFrame:
    mensal_tipo = (
        df.groupby(["ano_mes", "tipo"], as_index=False)["valor"]
        .sum()
        .pivot(index="ano_mes", columns="tipo", values="valor")
        .fillna(0)
        .reset_index()
    )

    if "receita" not in mensal_tipo.columns:
        mensal_tipo["receita"] = 0.0
    if "despesa" not in mensal_tipo.columns:
        mensal_tipo["despesa"] = 0.0

    mensal_tipo["saldo"] = mensal_tipo["receita"] - mensal_tipo["despesa"]
    mensal_tipo = mensal_tipo.rename(
        columns={"receita": "total_receita", "despesa": "total_despesa"}
    )
    return mensal_tipo.sort_values("ano_mes")


def gerar_resumo_categoria(df: pd.DataFrame) -> pd.DataFrame:
    resumo = (
        df.groupby(["ano_mes", "categoria", "tipo"], as_index=False)["valor"]
        .sum()
        .sort_values(["ano_mes", "tipo", "valor"], ascending=[True, True, False])
    )
    return resumo


def gerar_indicadores(df: pd.DataFrame, tempo_manual_min: float = 2.5, tempo_auto_min: float = 0.3) -> pd.DataFrame:
    total_lancamentos = len(df)
    horas_manuais = (total_lancamentos * tempo_manual_min) / 60
    horas_automatizadas = (total_lancamentos * tempo_auto_min) / 60
    horas_economizadas = max(horas_manuais - horas_automatizadas, 0)
    reducao_percentual = 0.0
    if horas_manuais > 0:
        reducao_percentual = (horas_economizadas / horas_manuais) * 100

    indicadores = pd.DataFrame(
        [
            {"indicador": "total_lancamentos", "valor": total_lancamentos},
            {"indicador": "horas_manuais_estimadas", "valor": round(horas_manuais, 2)},
            {"indicador": "horas_automatizadas_estimadas", "valor": round(horas_automatizadas, 2)},
            {"indicador": "horas_economizadas", "valor": round(horas_economizadas, 2)},
            {"indicador": "reducao_tempo_percentual", "valor": round(reducao_percentual, 2)},
        ]
    )
    return indicadores


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
    pasta_saida.mkdir(parents=True, exist_ok=True)
    arquivo_md = pasta_saida / "05_relatorio_executivo.md"

    receita_total = float(mensal["total_receita"].sum())
    despesa_total = float(mensal["total_despesa"].sum())
    saldo_total = float(mensal["saldo"].sum())
    meses_positivos = int((mensal["saldo"] > 0).sum())
    meses_total = len(mensal)

    mapa_indicadores = dict(zip(indicadores["indicador"], indicadores["valor"]))
    reducao_tempo = float(mapa_indicadores.get("reducao_tempo_percentual", 0.0))
    horas_economizadas = float(mapa_indicadores.get("horas_economizadas", 0.0))

    top_despesas = (
        categoria[categoria["tipo"] == "despesa"]
        .groupby("categoria", as_index=False)["valor"]
        .sum()
        .sort_values("valor", ascending=False)
        .head(5)
    )

    linhas_despesas = []
    for _, row in top_despesas.iterrows():
        linhas_despesas.append(f"- {row['categoria']}: {formatar_moeda_brl(float(row['valor']))}")

    linhas_mensal = []
    for _, row in mensal.iterrows():
        linhas_mensal.append(
            "| "
            f"{row['ano_mes']} | "
            f"{formatar_moeda_brl(float(row['total_receita']))} | "
            f"{formatar_moeda_brl(float(row['total_despesa']))} | "
            f"{formatar_moeda_brl(float(row['saldo']))} |"
        )

    hoje = date.today().strftime("%d/%m/%Y")

    texto = [
        "<div align='center'>",
        f"  <img src='{logo_path}' alt='Logotipo' width='180'>",
        f"  <h1>{titulo}</h1>",
        "  <p><strong>Entrega Executiva de Automacao Financeira</strong></p>",
        f"  <p>{nome_profissional} | {cargo_profissional}</p>",
        f"  <p>{empresa}</p>",
        f"  <p>Data de emissao: {hoje}</p>",
        "</div>",
        "",
        "---",
        "",
        f"# {titulo}",
        "",
        "## 1. Objetivo",
        "Demonstrar automacao de rotinas operacionais financeiras com foco em ganho de produtividade e padronizacao da gestao.",
        "",
        "## 2. Indicadores-Chave",
        f"- Receita total consolidada: {formatar_moeda_brl(receita_total)}",
        f"- Despesa total consolidada: {formatar_moeda_brl(despesa_total)}",
        f"- Saldo acumulado: {formatar_moeda_brl(saldo_total)}",
        f"- Meses com saldo positivo: {meses_positivos} de {meses_total}",
        f"- Reducao estimada de tempo operacional: {reducao_tempo:.2f}%",
        f"- Horas economizadas no periodo: {horas_economizadas:.2f}h",
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
        "A automatizacao elimina consolidacoes manuais, reduz retrabalho e garante consistencia de indicadores em todos os ciclos de fechamento.",
        "",
        "## 6. Entregaveis",
        "- 01_base_padronizada.csv",
        "- 02_resumo_mensal.csv",
        "- 03_resumo_categoria.csv",
        "- 04_indicadores_eficiencia.csv",
        "- 05_relatorio_executivo.md",
    ]

    arquivo_md.write_text("\n".join(texto), encoding="utf-8")
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
        pasta_site.mkdir(parents=True, exist_ok=True)
        arquivo_html = pasta_site / "index.html"

        receita_total = float(mensal["total_receita"].sum())
        despesa_total = float(mensal["total_despesa"].sum())
        saldo_total = float(mensal["saldo"].sum())
        meses_positivos = int((mensal["saldo"] > 0).sum())

        mapa_indicadores = dict(zip(indicadores["indicador"], indicadores["valor"]))
        reducao_tempo = float(mapa_indicadores.get("reducao_tempo_percentual", 0.0))
        horas_economizadas = float(mapa_indicadores.get("horas_economizadas", 0.0))

        top_despesas = (
                categoria[categoria["tipo"] == "despesa"]
                .groupby("categoria", as_index=False)["valor"]
                .sum()
                .sort_values("valor", ascending=False)
                .head(5)
        )

        linhas_mensal = []
        for _, row in mensal.iterrows():
                linhas_mensal.append(
                        "<tr>"
                        f"<td>{row['ano_mes']}</td>"
                        f"<td>{formatar_moeda_brl(float(row['total_receita']))}</td>"
                        f"<td>{formatar_moeda_brl(float(row['total_despesa']))}</td>"
                        f"<td>{formatar_moeda_brl(float(row['saldo']))}</td>"
                        "</tr>"
                )

        linhas_despesa = []
        for _, row in top_despesas.iterrows():
                linhas_despesa.append(f"<li><strong>{row['categoria']}</strong>: {formatar_moeda_brl(float(row['valor']))}</li>")

        hoje = date.today().strftime("%d/%m/%Y")
        html = f"""<!doctype html>
<html lang=\"pt-BR\">
<head>
    <meta charset=\"utf-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
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
        .container {{
            max-width: 1040px;
            margin: 0 auto;
            padding: 24px 16px 48px;
        }}
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
    <main class=\"container\">
        <section class=\"hero\">
            <img src=\"{logo_path}\" alt=\"Logotipo\">
            <h1>{titulo}</h1>
            <p><strong>Entrega Executiva de Automacao Financeira</strong></p>
            <p>{nome_profissional} | {cargo_profissional}</p>
            <p>{empresa}</p>
            <p>Data de emissao: {hoje}</p>
        </section>

        <section class=\"grid\">
            <article class=\"kpi\"><small>Receita Total</small><strong>{formatar_moeda_brl(receita_total)}</strong></article>
            <article class=\"kpi\"><small>Despesa Total</small><strong>{formatar_moeda_brl(despesa_total)}</strong></article>
            <article class=\"kpi\"><small>Saldo Acumulado</small><strong class=\"{'saldo-ok' if saldo_total >= 0 else 'saldo-alerta'}\">{formatar_moeda_brl(saldo_total)}</strong></article>
            <article class=\"kpi\"><small>Meses Positivos</small><strong>{meses_positivos}</strong></article>
            <article class=\"kpi\"><small>Reducao de Tempo</small><strong>{reducao_tempo:.2f}%</strong></article>
            <article class=\"kpi\"><small>Horas Economizadas</small><strong>{horas_economizadas:.2f}h</strong></article>
        </section>

        <section class=\"panel\">
            <h2>Painel Mensal Padronizado</h2>
            <table>
                <thead>
                    <tr><th>Mes</th><th>Receita</th><th>Despesa</th><th>Saldo</th></tr>
                </thead>
                <tbody>
                    {''.join(linhas_mensal)}
                </tbody>
            </table>
        </section>

        <section class=\"panel\">
            <h2>Top 5 Categorias de Despesa</h2>
            <ul>{''.join(linhas_despesa)}</ul>
        </section>

        <section class=\"panel\">
            <h2>Resultado do Projeto</h2>
            <p>A automacao elimina consolidacoes manuais, reduz retrabalho e padroniza indicadores para tomada de decisao em ciclos de fechamento.</p>
            <p class=\"foot\">Publicacao web pronta para GitHub Pages a partir da pasta docs.</p>
        </section>
    </main>
</body>
</html>
"""

        arquivo_html.write_text(html, encoding="utf-8")
        return arquivo_html


def salvar_relatorios(base: pd.DataFrame, mensal: pd.DataFrame, categoria: pd.DataFrame, indicadores: pd.DataFrame, pasta_saida: Path) -> bool:
    pasta_saida.mkdir(parents=True, exist_ok=True)

    base.to_csv(pasta_saida / "01_base_padronizada.csv", index=False)
    mensal.to_csv(pasta_saida / "02_resumo_mensal.csv", index=False)
    categoria.to_csv(pasta_saida / "03_resumo_categoria.csv", index=False)
    indicadores.to_csv(pasta_saida / "04_indicadores_eficiencia.csv", index=False)

    arquivo_excel = pasta_saida / "relatorio_gerencial_padronizado.xlsx"
    try:
        with pd.ExcelWriter(arquivo_excel) as writer:
            base.to_excel(writer, sheet_name="base_padronizada", index=False)
            mensal.to_excel(writer, sheet_name="resumo_mensal", index=False)
            categoria.to_excel(writer, sheet_name="resumo_categoria", index=False)
            indicadores.to_excel(writer, sheet_name="indicadores", index=False)
        return True
    except ModuleNotFoundError:
        print("Aviso: openpyxl nao instalado. Arquivo Excel nao foi gerado.")
        print("Para habilitar, instale com: pip install openpyxl")
        return False


def imprimir_painel(mensal: pd.DataFrame, indicadores: pd.DataFrame) -> None:
    print("\n" + "=" * 72)
    print("AUTOMACAO DE RELATORIOS FINANCEIROS")
    print("=" * 72)

    print("\nResumo mensal:")
    print(mensal.to_string(index=False))

    print("\nIndicadores de eficiencia:")
    print(indicadores.to_string(index=False))
    print("=" * 72)


def criar_exemplo(arquivo: Path) -> None:
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Portfolio: automacao de rotinas financeiras e padronizacao de relatorios gerenciais"
    )
    parser.add_argument(
        "--entrada",
        default="dados/lancamentos_financeiros.csv",
        help="Arquivo CSV com lancamentos financeiros",
    )
    parser.add_argument(
        "--saida",
        default="relatorios",
        help="Diretorio de saida dos relatorios",
    )
    parser.add_argument(
        "--gerar-exemplo",
        action="store_true",
        help="Gera um arquivo de exemplo caso ainda nao exista",
    )
    parser.add_argument(
        "--titulo-relatorio",
        default="Portfolio Executivo - Sara",
        help="Titulo do relatorio executivo em Markdown",
    )
    parser.add_argument(
        "--nome-profissional",
        default="Sara",
        help="Nome para capa do relatorio",
    )
    parser.add_argument(
        "--cargo-profissional",
        default="Analista de Automacao Financeira",
        help="Cargo para capa do relatorio",
    )
    parser.add_argument(
        "--empresa",
        default="Analise Criterio",
        help="Empresa ou projeto para capa do relatorio",
    )
    parser.add_argument(
        "--logo",
        default="../assets/logo.svg",
        help="Caminho do logotipo usado na capa do markdown",
    )
    parser.add_argument(
        "--site-dir",
        default="docs",
        help="Diretorio para site HTML publicado no GitHub Pages",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    arquivo_entrada = Path(args.entrada)
    pasta_saida = Path(args.saida)
    pasta_site = Path(args.site_dir)

    if args.gerar_exemplo and not arquivo_entrada.exists():
        criar_exemplo(arquivo_entrada)
        print(f"Arquivo de exemplo criado em: {arquivo_entrada}")

    if not arquivo_entrada.exists():
        raise FileNotFoundError(
            f"Arquivo de entrada nao encontrado: {arquivo_entrada}. "
            "Use --gerar-exemplo para criar uma base inicial."
        )

    bruto = pd.read_csv(arquivo_entrada)
    base = limpar_dados(normalizar_colunas(bruto))

    if base.empty:
        raise ValueError("Nenhum lancamento valido encontrado apos limpeza dos dados.")

    resumo_mensal = gerar_resumo_mensal(base)
    resumo_categoria = gerar_resumo_categoria(base)
    indicadores = gerar_indicadores(base)

    excel_gerado = salvar_relatorios(base, resumo_mensal, resumo_categoria, indicadores, pasta_saida)
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

    print("\nRelatorios gerados com sucesso:")
    print(f"- {pasta_saida / '01_base_padronizada.csv'}")
    print(f"- {pasta_saida / '02_resumo_mensal.csv'}")
    print(f"- {pasta_saida / '03_resumo_categoria.csv'}")
    print(f"- {pasta_saida / '04_indicadores_eficiencia.csv'}")
    print(f"- {arquivo_md}")
    print(f"- {arquivo_html}")
    if excel_gerado:
        print(f"- {pasta_saida / 'relatorio_gerencial_padronizado.xlsx'}")


if __name__ == "__main__":
    main()