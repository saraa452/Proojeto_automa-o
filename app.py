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
import json
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

EXEMPLO_LANCAMENTOS: list[list[object]] = [
    ["08/01/2026", "Mensalidade carteira premium", "assinaturas", "receita", 8400.00, "comercial"],
    ["12/01/2026", "Projeto de implantacao alfa", "projetos", "receita", 12600.00, "consultoria"],
    ["15/01/2026", "Midia de aquisicao", "marketing", "despesa", 1850.45, "marketing"],
    ["20/01/2026", "Infraestrutura em nuvem", "tecnologia", "despesa", 920.30, "ti"],
    ["28/01/2026", "Folha operacional", "pessoal", "despesa", 5400.00, "operacoes"],
    ["05/02/2026", "Renovacao contratos B2B", "assinaturas", "receita", 9100.00, "comercial"],
    ["09/02/2026", "Treinamento corporativo", "servicos", "receita", 5200.00, "educacao"],
    ["13/02/2026", "Fornecedor homologado", "fornecedores", "despesa", 4100.80, "operacoes"],
    ["18/02/2026", "Ferramentas analiticas", "tecnologia", "despesa", 1180.25, "ti"],
    ["26/02/2026", "Comissao comercial", "pessoal", "despesa", 2890.00, "comercial"],
    ["04/03/2026", "Projeto de integracao beta", "projetos", "receita", 14850.00, "consultoria"],
    ["11/03/2026", "Receita de suporte premium", "servicos", "receita", 4650.00, "sucesso_cliente"],
    ["14/03/2026", "Campanha de retencao", "marketing", "despesa", 2310.10, "marketing"],
    ["19/03/2026", "Licencas de software", "tecnologia", "despesa", 1340.60, "ti"],
    ["27/03/2026", "Folha salarial", "pessoal", "despesa", 6120.00, "rh"],
    ["02/04/2026", "Pacote de consultoria recorrente", "servicos", "receita", 6900.00, "consultoria"],
    ["08/04/2026", "Expansao de contas enterprise", "vendas", "receita", 11800.00, "comercial"],
    ["15/04/2026", "Viagens comerciais", "operacoes", "despesa", 1740.55, "comercial"],
    ["18/04/2026", "Servicos terceirizados", "fornecedores", "despesa", 4525.00, "operacoes"],
    ["24/04/2026", "Energia e utilidades", "infraestrutura", "despesa", 860.90, "adm"],
    ["06/05/2026", "Novos contratos SMB", "vendas", "receita", 9700.00, "comercial"],
    ["10/05/2026", "Receita de onboarding", "servicos", "receita", 4300.00, "educacao"],
    ["13/05/2026", "Eventos e comunidade", "marketing", "despesa", 2088.40, "marketing"],
    ["19/05/2026", "Upgrade de seguranca", "tecnologia", "despesa", 980.15, "ti"],
    ["28/05/2026", "Beneficios e encargos", "pessoal", "despesa", 6335.20, "rh"],
    ["03/06/2026", "Projeto analytics gamma", "projetos", "receita", 16300.00, "consultoria"],
    ["09/06/2026", "Receita de renovacao anual", "assinaturas", "receita", 8800.00, "comercial"],
    ["12/06/2026", "Parceiros estrategicos", "fornecedores", "despesa", 3895.00, "operacoes"],
    ["17/06/2026", "Monitoramento de plataforma", "tecnologia", "despesa", 1125.80, "ti"],
    ["26/06/2026", "Aluguel escritorio", "infraestrutura", "despesa", 2790.00, "adm"],
    ["07/07/2026", "Squad dedicado delta", "projetos", "receita", 17150.00, "consultoria"],
    ["11/07/2026", "Pacote de suporte enterprise", "servicos", "receita", 5400.00, "sucesso_cliente"],
    ["15/07/2026", "Recrutamento especializado", "pessoal", "despesa", 6840.00, "rh"],
    ["21/07/2026", "Campanha ABM", "marketing", "despesa", 2645.70, "marketing"],
    ["29/07/2026", "Automacao de backoffice", "tecnologia", "despesa", 1495.35, "ti"],
    ["05/08/2026", "Renovacao base enterprise", "assinaturas", "receita", 10200.00, "comercial"],
    ["08/08/2026", "Projeto de performance epsilon", "projetos", "receita", 15400.00, "consultoria"],
    ["14/08/2026", "Operacao de campo", "operacoes", "despesa", 1985.25, "operacoes"],
    ["20/08/2026", "Compliance e auditoria", "infraestrutura", "despesa", 1320.00, "adm"],
    ["27/08/2026", "Folha variavel e bonus", "pessoal", "despesa", 7210.00, "rh"],
    ["30/01/2026", "Desconto comercial campanha janeiro", "descontos_comerciais", "despesa", 420.00, "comercial"],
    ["27/02/2026", "Desconto promocional fevereiro", "descontos_comerciais", "despesa", 390.00, "comercial"],
    ["30/03/2026", "Desconto negociacao marco", "descontos_comerciais", "despesa", 430.00, "comercial"],
    ["29/04/2026", "Desconto upgrade abril", "descontos_comerciais", "despesa", 410.00, "comercial"],
    ["30/05/2026", "Desconto pacote maio", "descontos_comerciais", "despesa", 395.00, "comercial"],
    ["29/06/2026", "Desconto renovacao junho", "descontos_comerciais", "despesa", 460.00, "comercial"],
    ["30/07/2026", "Desconto volume julho", "descontos_comerciais", "despesa", 450.00, "comercial"],
    ["29/08/2026", "Desconto sazonal agosto", "descontos_comerciais", "despesa", 470.00, "comercial"],
    ["03/09/2026", "Projeto de expansao zeta", "projetos", "receita", 18200.00, "consultoria"],
    ["09/09/2026", "Pacote premium anual", "assinaturas", "receita", 11200.00, "comercial"],
    ["16/09/2026", "Midia performance", "marketing", "despesa", 2380.00, "marketing"],
    ["22/09/2026", "Operacao de suporte", "operacoes", "despesa", 2140.00, "operacoes"],
    ["29/09/2026", "Desconto proposta setembro", "descontos_comerciais", "despesa", 520.00, "comercial"],
    ["07/10/2026", "Projeto enterprise eta", "projetos", "receita", 19400.00, "consultoria"],
    ["12/10/2026", "Receita de sucesso do cliente", "servicos", "receita", 6100.00, "sucesso_cliente"],
    ["18/10/2026", "Licencas e compliance", "tecnologia", "despesa", 1650.00, "ti"],
    ["24/10/2026", "Despesa operacional outubro", "operacoes", "despesa", 2260.00, "operacoes"],
    ["30/10/2026", "Desconto contratual outubro", "descontos_comerciais", "despesa", 580.00, "comercial"],
    ["05/11/2026", "Projeto analytics theta", "projetos", "receita", 20100.00, "consultoria"],
    ["11/11/2026", "Renovacao carteira enterprise", "assinaturas", "receita", 11800.00, "comercial"],
    ["17/11/2026", "Folha e encargos novembro", "pessoal", "despesa", 7420.00, "rh"],
    ["23/11/2026", "Infraestrutura e energia", "infraestrutura", "despesa", 1580.00, "adm"],
    ["29/11/2026", "Desconto black november", "descontos_comerciais", "despesa", 760.00, "comercial"],
    ["04/12/2026", "Projeto fechamento anual iota", "projetos", "receita", 22300.00, "consultoria"],
    ["10/12/2026", "Receita adicional de servicos", "servicos", "receita", 6900.00, "educacao"],
    ["16/12/2026", "Campanha institucional", "marketing", "despesa", 2480.00, "marketing"],
    ["22/12/2026", "Operacao e logistica dezembro", "operacoes", "despesa", 2320.00, "operacoes"],
    ["29/12/2026", "Desconto fechamento dezembro", "descontos_comerciais", "despesa", 820.00, "comercial"],
    ["08/01/2027", "Renovacao enterprise janeiro", "assinaturas", "receita", 12400.00, "comercial"],
    ["13/01/2027", "Projeto performance lambda", "projetos", "receita", 23100.00, "consultoria"],
    ["19/01/2027", "Campanha digital janeiro", "marketing", "despesa", 2750.00, "marketing"],
    ["24/01/2027", "Operacao distribuida janeiro", "operacoes", "despesa", 2460.00, "operacoes"],
    ["30/01/2027", "Desconto comercial janeiro 2027", "descontos_comerciais", "despesa", 690.00, "comercial"],
    ["06/02/2027", "Pacote premium fevereiro", "assinaturas", "receita", 12900.00, "comercial"],
    ["10/02/2027", "Projeto transformacao mu", "projetos", "receita", 23800.00, "consultoria"],
    ["16/02/2027", "Licencas e dados fevereiro", "tecnologia", "despesa", 1890.00, "ti"],
    ["22/02/2027", "Folha operacional fevereiro", "pessoal", "despesa", 7680.00, "rh"],
    ["27/02/2027", "Desconto campanha fevereiro 2027", "descontos_comerciais", "despesa", 710.00, "comercial"],
    ["05/03/2027", "Receita onboarding corporativo", "servicos", "receita", 8200.00, "educacao"],
    ["11/03/2027", "Projeto analytics nu", "projetos", "receita", 24500.00, "consultoria"],
    ["17/03/2027", "Midia de aquisicao marco", "marketing", "despesa", 2860.00, "marketing"],
    ["23/03/2027", "Infraestrutura cloud marco", "infraestrutura", "despesa", 1760.00, "adm"],
    ["29/03/2027", "Desconto negociacao marco 2027", "descontos_comerciais", "despesa", 730.00, "comercial"],
    ["07/04/2027", "Receita suporte premium abril", "servicos", "receita", 8600.00, "sucesso_cliente"],
    ["12/04/2027", "Projeto expansao xi", "projetos", "receita", 25200.00, "consultoria"],
    ["18/04/2027", "Compliance e auditoria abril", "infraestrutura", "despesa", 1690.00, "adm"],
    ["24/04/2027", "Operacao de campo abril", "operacoes", "despesa", 2590.00, "operacoes"],
    ["29/04/2027", "Desconto upgrade abril 2027", "descontos_comerciais", "despesa", 760.00, "comercial"],
    ["06/05/2027", "Renovacao carteira maio", "assinaturas", "receita", 13300.00, "comercial"],
    ["10/05/2027", "Projeto dados omicron", "projetos", "receita", 26100.00, "consultoria"],
    ["16/05/2027", "Eventos e comunidade maio", "marketing", "despesa", 2980.00, "marketing"],
    ["21/05/2027", "Folha e encargos maio", "pessoal", "despesa", 7890.00, "rh"],
    ["28/05/2027", "Desconto promocional maio 2027", "descontos_comerciais", "despesa", 780.00, "comercial"],
    ["04/06/2027", "Receita servicos especiais junho", "servicos", "receita", 9100.00, "educacao"],
    ["09/06/2027", "Projeto consolidacao pi", "projetos", "receita", 26800.00, "consultoria"],
    ["15/06/2027", "Monitoramento de plataforma junho", "tecnologia", "despesa", 1980.00, "ti"],
    ["22/06/2027", "Parceiros estrategicos junho", "fornecedores", "despesa", 4360.00, "operacoes"],
    ["29/06/2027", "Desconto renovacao junho 2027", "descontos_comerciais", "despesa", 820.00, "comercial"],
]


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


def _agrupar_financeiro(df: pd.DataFrame, dimensoes: list[str]) -> pd.DataFrame:
    """Agrupa receitas e despesas em torno de uma ou mais dimensoes."""
    resumo = (
        df.groupby([*dimensoes, "tipo"], as_index=False)["valor"]
        .sum()
        .pivot(index=dimensoes, columns="tipo", values="valor")
        .fillna(0)
        .reset_index()
    )

    if "receita" not in resumo.columns:
        resumo["receita"] = 0.0
    if "despesa" not in resumo.columns:
        resumo["despesa"] = 0.0

    resumo = resumo.rename(
        columns={"receita": "total_receita", "despesa": "total_despesa"}
    )
    resumo["saldo"] = resumo["total_receita"] - resumo["total_despesa"]
    return resumo.reset_index(drop=True)


def gerar_resumo_categoria_analitico(df: pd.DataFrame) -> pd.DataFrame:
    """Consolida indicadores por categoria para analise interativa."""
    resumo = _agrupar_financeiro(df, ["categoria"])
    transacoes = df.groupby("categoria", as_index=False).size().rename(
        columns={"size": "transacoes"}
    )
    total_movimentado = df.groupby("categoria", as_index=False).agg(
        total_movimentado=("valor", "sum")
    )
    resumo = resumo.merge(transacoes, on="categoria").merge(total_movimentado, on="categoria")
    resumo["ticket_medio"] = (resumo["total_movimentado"] / resumo["transacoes"]).round(2)

    total_despesa = float(resumo["total_despesa"].sum())
    resumo["participacao_despesa"] = resumo["total_despesa"].apply(
        lambda valor: round((valor / total_despesa) * 100, 2) if total_despesa > 0 else 0.0
    )
    return resumo.sort_values(["total_despesa", "saldo"], ascending=[False, False]).reset_index(drop=True)


def gerar_resumo_centro_custo(df: pd.DataFrame) -> pd.DataFrame:
    """Consolida indicadores por centro de custo para analise interativa."""
    resumo = _agrupar_financeiro(df, ["centro_custo"])
    transacoes = df.groupby("centro_custo", as_index=False).size().rename(
        columns={"size": "transacoes"}
    )
    resumo = resumo.merge(transacoes, on="centro_custo")
    resumo["ticket_medio"] = (
        (resumo["total_receita"] + resumo["total_despesa"]) / resumo["transacoes"]
    ).round(2)
    return resumo.sort_values(["total_despesa", "saldo"], ascending=[False, False]).reset_index(drop=True)


def gerar_eficiencia_mensal(
    df: pd.DataFrame,
    tempo_manual_min: float = TEMPO_MANUAL_MIN,
    tempo_auto_min: float = TEMPO_AUTO_MIN,
) -> pd.DataFrame:
    """Calcula produtividade operacional por mes."""
    eficiencia = df.groupby("ano_mes", as_index=False).size().rename(
        columns={"size": "lancamentos"}
    )
    eficiencia["horas_manuais"] = (
        eficiencia["lancamentos"] * tempo_manual_min / 60
    ).round(2)
    eficiencia["horas_automatizadas"] = (
        eficiencia["lancamentos"] * tempo_auto_min / 60
    ).round(2)
    eficiencia["horas_economizadas"] = (
        eficiencia["horas_manuais"] - eficiencia["horas_automatizadas"]
    ).round(2)
    eficiencia["ganho_percentual"] = eficiencia.apply(
        lambda row: round(
            (row["horas_economizadas"] / row["horas_manuais"]) * 100,
            2,
        )
        if row["horas_manuais"] > 0
        else 0.0,
        axis=1,
    )
    return eficiencia.sort_values("ano_mes").reset_index(drop=True)


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


def montar_payload_site(
    base: pd.DataFrame,
    mensal: pd.DataFrame,
    categoria: pd.DataFrame,
    indicadores: pd.DataFrame,
) -> dict[str, object]:
    """Monta os dados-base serializados para o dashboard interativo."""
    registros = base.copy()
    registros["data"] = registros["data"].dt.strftime("%Y-%m-%d")
    registros["valor"] = registros["valor"].astype(float).round(2)
    registros["valor_assinado"] = registros["valor_assinado"].astype(float).round(2)

    return {
        "generatedAt": date.today().isoformat(),
        "tempos": {
            "manual_min": TEMPO_MANUAL_MIN,
            "auto_min": TEMPO_AUTO_MIN,
        },
        "records": registros[
            [
                "data",
                "ano_mes",
                "descricao",
                "categoria",
                "tipo",
                "valor",
                "centro_custo",
                "valor_assinado",
            ]
        ].to_dict(orient="records"),
    }


def salvar_payload_site(payload: dict[str, object], pasta_site: Path) -> Path:
    """Salva o payload externo consumido pela pagina HTML."""
    pasta_assets = pasta_site / "assets"
    pasta_assets.mkdir(parents=True, exist_ok=True)
    arquivo_json = pasta_assets / "dashboard-data.json"
    arquivo_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("Payload JSON do site gerado: %s", arquivo_json)
    return arquivo_json


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
    base: pd.DataFrame,
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
    hoje = date.today().strftime("%d/%m/%Y")
    logo_site = _preparar_logo_site(logo_path, pasta_site)
    payload_site = montar_payload_site(
        base=base,
        mensal=mensal,
        categoria=categoria,
        indicadores=indicadores,
    )
    salvar_payload_site(payload_site, pasta_site)
    classe_saldo = "saldo-ok" if kpis["saldo_total"] >= 0 else "saldo-alerta"

    html = f"""<!doctype html>
<html lang="pt-BR">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{titulo}</title>
    <style>
        :root {{
            --bg: #f4efe6;
            --surface: rgba(255, 251, 245, 0.82);
            --card: #fffdf8;
            --ink: #1b1b1b;
            --muted: #6e6258;
            --brand: #17313e;
            --accent: #c46a2f;
            --accent-soft: #f0d2b8;
            --line: rgba(23, 49, 62, 0.12);
            --ok: #166534;
            --warn: #9a3412;
            --shadow: 0 18px 40px rgba(23, 49, 62, 0.14);
        }}
        * {{ box-sizing: border-box; }}
        body {{
            margin: 0;
            font-family: "Aptos", "Trebuchet MS", "Segoe UI", sans-serif;
            color: var(--ink);
            background:
                radial-gradient(circle at 12% 18%, rgba(196, 106, 47, 0.18) 0%, transparent 33%),
                radial-gradient(circle at 88% 8%, rgba(23, 49, 62, 0.14) 0%, transparent 28%),
                linear-gradient(180deg, #f7f1e7 0%, #f4efe6 42%, #efe3d2 100%),
                var(--bg);
            line-height: 1.5;
        }}
        .container {{ max-width: 1180px; margin: 0 auto; padding: 28px 18px 56px; }}
        .hero {{
            position: relative;
            overflow: hidden;
            background: linear-gradient(140deg, #17313e 0%, #28586c 52%, #c46a2f 100%);
            color: #fff7f0;
            border-radius: 28px;
            padding: 30px;
            box-shadow: var(--shadow);
        }}
        .hero::after {{
            content: "";
            position: absolute;
            inset: auto -40px -60px auto;
            width: 220px;
            height: 220px;
            border-radius: 50%;
            background: rgba(255, 255, 255, 0.12);
            filter: blur(4px);
        }}
        .hero-top {{ display: flex; justify-content: space-between; gap: 18px; align-items: flex-start; }}
        .hero-copy {{ max-width: 760px; position: relative; z-index: 1; }}
        .hero img {{ width: 124px; background: #fff8f1; border-radius: 16px; padding: 10px; }}
        .eyebrow {{ text-transform: uppercase; letter-spacing: 0.18em; font-size: 0.76rem; opacity: 0.8; }}
        .hero h1 {{ margin: 10px 0 8px; font-size: clamp(1.9rem, 4vw, 3.25rem); line-height: 1.04; max-width: 11ch; }}
        .hero p {{ margin: 5px 0; opacity: 0.96; }}
        .hero-meta {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 18px; }}
        .hero-chip {{
            padding: 10px 14px;
            border: 1px solid rgba(255, 255, 255, 0.18);
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.09);
            backdrop-filter: blur(8px);
        }}
        .grid {{
            display: grid;
            gap: 14px;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        }}
        .kpi {{
            background: linear-gradient(180deg, #fffdf8 0%, #fff7ef 100%);
            border: 1px solid var(--line);
            border-radius: 18px;
            padding: 16px;
            box-shadow: 0 8px 20px rgba(23, 49, 62, 0.06);
        }}
        .kpi small {{ color: var(--muted); display: block; margin-bottom: 8px; }}
        .kpi strong {{ font-size: 1.2rem; line-height: 1.2; }}
        .panel {{
            margin-top: 18px;
            background: var(--surface);
            backdrop-filter: blur(18px);
            border: 1px solid var(--line);
            border-radius: 24px;
            padding: 20px;
            box-shadow: var(--shadow);
        }}
        h2 {{ margin: 0 0 8px; color: var(--brand); font-size: clamp(1.25rem, 2vw, 1.7rem); }}
        .panel-head {{ display: flex; justify-content: space-between; gap: 16px; align-items: end; margin-bottom: 18px; }}
        .panel-head p {{ margin: 0; max-width: 720px; color: var(--muted); }}
        .controls {{ display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); margin-top: 18px; }}
        .field {{ display: flex; flex-direction: column; gap: 8px; }}
        .field label {{ font-size: 0.85rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; }}
        .filter-grid {{
            margin-top: 14px;
            display: grid;
            gap: 10px;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        }}
        select {{
            width: 100%;
            padding: 12px 14px;
            border-radius: 14px;
            border: 1px solid var(--line);
            background: #fffaf4;
            color: var(--ink);
            font: inherit;
        }}
        .stage-nav {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 18px; }}
        .stage-button {{
            border: 0;
            border-radius: 999px;
            padding: 10px 16px;
            font: inherit;
            background: rgba(23, 49, 62, 0.08);
            color: var(--brand);
            cursor: pointer;
            transition: transform 160ms ease, background 160ms ease, color 160ms ease;
        }}
        .stage-button:hover {{ transform: translateY(-1px); }}
        .stage-button.active {{ background: var(--brand); color: #fff8f2; }}
        .dashboard-layout {{ display: grid; gap: 18px; grid-template-columns: 1.2fr 0.8fr; align-items: start; }}
        .chart-card {{
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 20px;
            padding: 18px;
        }}
        .canvas-wrap {{
            border: 1px solid var(--line);
            border-radius: 16px;
            background: #fffaf4;
            padding: 10px;
        }}
        #bars-canvas {{
            width: 100%;
            height: 320px;
            display: block;
        }}
        .trend-box {{
            margin-top: 12px;
            border: 1px solid var(--line);
            border-radius: 16px;
            padding: 10px;
            background: #fffaf4;
        }}
        .trend-title {{ font-size: 0.84rem; color: var(--muted); margin-bottom: 6px; }}
        #trend-svg {{ width: 100%; height: 120px; display: block; }}
        .chart-meta {{ display: flex; justify-content: space-between; gap: 12px; align-items: baseline; margin-bottom: 14px; }}
        .chart-meta h3, .table-card h3 {{ margin: 0; color: var(--brand); font-size: 1.05rem; }}
        .chart-meta p {{ margin: 0; color: var(--muted); font-size: 0.93rem; }}
        .table-card {{
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 20px;
            padding: 18px;
            overflow: hidden;
        }}
        .table-wrap {{ overflow-x: auto; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ border-bottom: 1px solid var(--line); padding: 10px 8px; text-align: left; white-space: nowrap; }}
        th {{ background: #f7eee4; color: var(--brand); }}
        .summary-text {{ color: var(--muted); max-width: 760px; }}
        .narrative {{ display: grid; gap: 14px; }}
        .narrative h3 {{ margin: 0; color: var(--brand); font-size: 1.03rem; }}
        .narrative p {{ margin: 6px 0 0; color: var(--muted); }}
        .narrative ul {{ margin: 6px 0 0; color: var(--muted); }}
        .narrative li {{ margin: 4px 0; }}
        .divider {{ height: 1px; background: var(--line); margin: 4px 0; }}
        .highlight {{ margin-top: 16px; padding: 14px 16px; border-left: 4px solid var(--accent); background: rgba(196, 106, 47, 0.08); border-radius: 14px; color: var(--brand); }}
        .foot {{ margin-top: 18px; color: var(--muted); font-size: 0.92rem; }}
        .status {{ margin-top: 10px; color: var(--muted); font-size: 0.9rem; }}
        .saldo-ok {{ color: var(--ok); }}
        .saldo-alerta {{ color: var(--warn); }}
        @media (max-width: 840px) {{
            .hero-top {{ flex-direction: column-reverse; }}
            .dashboard-layout {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <main class="container">
        <section class="hero">
            <div class="hero-top">
                <div class="hero-copy">
                    <div class="eyebrow">Portfolio profissional | nivel consultoria</div>
                    <h1>Transformando dados em decisoes estrategicas</h1>
                    <p><strong>Projeto de automacao e analise financeira com foco em eficiencia operacional e suporte a tomada de decisao</strong></p>
                    <p>Sara Nascimento | Analista Administrativo e Financeiro</p>
                    <p>{empresa}</p>
                    <div class="hero-meta">
                        <div class="hero-chip">Consolidacao automatizada por periodo, categoria e centro de custo</div>
                        <div class="hero-chip">Saldo total: <span class="{classe_saldo}">{formatar_moeda_brl(kpis['saldo_total'])}</span></div>
                        <div class="hero-chip">Data de emissao: {hoje}</div>
                    </div>
                </div>
                <img src="{logo_site}" alt="Logotipo">
            </div>
        </section>

        <section class="panel">
            <div class="panel-head">
                <div>
                    <h2>Sobre o Projeto</h2>
                    <p class="summary-text">Estrutura de apresentacao focada em contexto de negocio, problema, solucao e resultados.</p>
                </div>
            </div>
            <div class="narrative">
                <section>
                    <h3>Contexto de negocio</h3>
                    <p>Este projeto foi desenvolvido para automatizar a consolidacao de dados financeiros e administrativos, reduzindo processos manuais e aumentando a confiabilidade das informacoes. A solucao integra dados de diferentes periodos e centros de custo, permitindo analise estruturada e geracao de indicadores estrategicos para apoio a tomada de decisao.</p>
                </section>
                <section>
                    <h3>Problema de negocio</h3>
                    <p>Antes da implementacao, a analise era manual, com alto risco de inconsistencias, retrabalho e baixa agilidade na geracao de relatorios. A ausencia de padronizacao dificultava comparacoes entre periodos e comprometia a visao estrategica das informacoes.</p>
                </section>
                <section>
                    <h3>Solucao desenvolvida</h3>
                    <p>Foi estruturado um fluxo automatizado de tratamento e analise de dados em etapas de transformacao, padronizacao e consolidacao. Em paralelo, foi construido um dashboard interativo para navegacao entre visoes analiticas e exploracao de indicadores sem reprocessamento.</p>
                </section>
                <section>
                    <h3>Tecnologias utilizadas</h3>
                    <ul>
                        <li>Python para tratamento e automacao de dados</li>
                        <li>Estrutura ETL (Extracao, Transformacao e Carga)</li>
                        <li>Modelagem de dados e agregacoes analiticas</li>
                        <li>Dashboard interativo (HTML, JavaScript, Canvas e SVG)</li>
                        <li>Git e GitHub para versionamento e publicacao</li>
                    </ul>
                </section>
                <section>
                    <h3>Principais analises</h3>
                    <ul>
                        <li>Receita bruta, receita liquida e descontos</li>
                        <li>Evolucao mensal de indicadores</li>
                        <li>Analise por categoria</li>
                        <li>Analise por centro de custo</li>
                        <li>Identificacao de padroes e variacoes</li>
                    </ul>
                </section>
            </div>
            <div class="divider"></div>
            <div class="controls">
                <div class="field">
                    <label for="stage-select">Etapa de analise</label>
                    <select id="stage-select"></select>
                </div>
                <div class="field">
                    <label for="index-select">Indice exibido</label>
                    <select id="index-select"></select>
                </div>
            </div>
            <div class="filter-grid">
                <div class="field">
                    <label for="mes-filter">Filtro de mes</label>
                    <select id="mes-filter"></select>
                </div>
                <div class="field">
                    <label for="categoria-filter">Filtro de categoria</label>
                    <select id="categoria-filter"></select>
                </div>
                <div class="field">
                    <label for="centro-filter">Filtro de centro de custo</label>
                    <select id="centro-filter"></select>
                </div>
            </div>
            <div class="stage-nav" id="stage-nav"></div>
            <p class="status" id="dataset-status">Carregando base externa...</p>
        </section>

        <section class="panel">
            <div class="panel-head">
                <div>
                    <h2 id="stage-title"></h2>
                    <p id="stage-description"></p>
                </div>
            </div>
            <section class="grid" id="cards-grid"></section>
            <div class="dashboard-layout" style="margin-top: 18px;">
                <article class="chart-card">
                    <div class="chart-meta">
                        <div>
                            <h3 id="chart-title"></h3>
                            <p id="chart-caption"></p>
                        </div>
                    </div>
                    <div class="canvas-wrap">
                        <canvas id="bars-canvas"></canvas>
                    </div>
                    <div class="trend-box">
                        <div class="trend-title">Leitura complementar em SVG</div>
                        <svg id="trend-svg" viewBox="0 0 540 120" preserveAspectRatio="none"></svg>
                    </div>
                    <div class="highlight" id="highlight"></div>
                </article>
                <article class="table-card">
                    <h3>Detalhamento da etapa</h3>
                    <div class="table-wrap">
                        <table>
                            <thead id="table-head"></thead>
                            <tbody id="table-body"></tbody>
                        </table>
                    </div>
                </article>
            </div>
        </section>

        <section class="panel">
            <h2>Resultados e Diferencial</h2>
            <p>
                A solucao proporcionou reducao significativa de atividades manuais, aumento da confiabilidade dos dados, maior rapidez na geracao de relatorios e melhor suporte a tomada de decisao.
            </p>
            <p>
                Diferente de analises estaticas, o dashboard permite interacao dinamica com multiplas visoes sem necessidade de reprocessamento. A estrutura foi desenhada para escalar e ser adaptada a diferentes contextos empresariais.
            </p>
            <p>
                Estou em busca de oportunidades como Analista Administrativo, Financeiro ou de Dados para aplicar habilidades em analise, automacao e geracao de insights estrategicos.
            </p>
            <p class="foot">Publicacao web pronta para GitHub Pages a partir da pasta docs.</p>
        </section>
    </main>
    <script>
        const DATA_URL = "assets/dashboard-data.json";
        const state = {{
            stageId: "visao_geral",
            indexId: null,
            mes: "todos",
            categoria: "todos",
            centro: "todos",
        }};
        const chartHeight = 320;
        let sourceData = null;
        let currentStages = null;
        let rawRecords = [];

        const stageSelect = document.getElementById("stage-select");
        const indexSelect = document.getElementById("index-select");
        const mesFilter = document.getElementById("mes-filter");
        const categoriaFilter = document.getElementById("categoria-filter");
        const centroFilter = document.getElementById("centro-filter");
        const datasetStatus = document.getElementById("dataset-status");
        const stageNav = document.getElementById("stage-nav");
        const stageTitle = document.getElementById("stage-title");
        const stageDescription = document.getElementById("stage-description");
        const cardsGrid = document.getElementById("cards-grid");
        const chartTitle = document.getElementById("chart-title");
        const chartCaption = document.getElementById("chart-caption");
        const barsCanvas = document.getElementById("bars-canvas");
        const trendSvg = document.getElementById("trend-svg");
        const tableHead = document.getElementById("table-head");
        const tableBody = document.getElementById("table-body");
        const highlight = document.getElementById("highlight");

        const formatters = {{
            currency: (value) => new Intl.NumberFormat("pt-BR", {{ style: "currency", currency: "BRL" }}).format(Number(value) || 0),
            percent: (value) => `${{Number(value || 0).toFixed(2)}}%`,
            hours: (value) => `${{Number(value || 0).toFixed(2)}}h`,
            int: (value) => new Intl.NumberFormat("pt-BR", {{ maximumFractionDigits: 0 }}).format(Number(value) || 0),
            text: (value) => value ?? "-",
        }};

        function formatValue(value, format) {{
            return (formatters[format] || formatters.text)(value);
        }}

        function calcularMetricasBase(records) {{
            const receita = records
                .filter((item) => item.tipo === "receita")
                .reduce((acc, item) => acc + Number(item.valor || 0), 0);
            const despesa = records
                .filter((item) => item.tipo === "despesa")
                .reduce((acc, item) => acc + Number(item.valor || 0), 0);
            return {{
                receita,
                despesa,
                saldo: receita - despesa,
            }};
        }}

        function agruparMensal(records) {{
            const mapa = new Map();
            records.forEach((item) => {{
                const chave = item.ano_mes;
                if (!mapa.has(chave)) {{
                    mapa.set(chave, {{
                        ano_mes: chave,
                        total_receita: 0,
                        total_despesa: 0,
                        saldo: 0,
                        lancamentos: 0,
                    }});
                }}

                const atual = mapa.get(chave);
                atual.lancamentos += 1;
                if (item.tipo === "receita") {{
                    atual.total_receita += Number(item.valor || 0);
                }} else {{
                    atual.total_despesa += Number(item.valor || 0);
                }}
                atual.saldo = atual.total_receita - atual.total_despesa;
            }});

            return Array.from(mapa.values())
                .sort((a, b) => a.ano_mes.localeCompare(b.ano_mes))
                .map((item) => ({{
                    ...item,
                    margem_percentual: item.total_receita > 0
                        ? Number(((item.saldo / item.total_receita) * 100).toFixed(2))
                        : 0,
                }}));
        }}

        function agruparReceitaDetalhada(records) {{
            const mapa = new Map();
            records.forEach((item) => {{
                const chave = item.ano_mes;
                if (!mapa.has(chave)) {{
                    mapa.set(chave, {{
                        ano_mes: chave,
                        receita_bruta: 0,
                        descontos: 0,
                        receita_liquida: 0,
                        taxa_desconto: 0,
                    }});
                }}

                const atual = mapa.get(chave);
                const valor = Number(item.valor || 0);
                if (item.tipo === "receita") {{
                    atual.receita_bruta += valor;
                }}
                if (item.tipo === "despesa" && String(item.categoria || "").toLowerCase().includes("desconto")) {{
                    atual.descontos += valor;
                }}
                atual.receita_liquida = atual.receita_bruta - atual.descontos;
                atual.taxa_desconto = atual.receita_bruta > 0
                    ? Number(((atual.descontos / atual.receita_bruta) * 100).toFixed(2))
                    : 0;
            }});

            return Array.from(mapa.values())
                .sort((a, b) => a.ano_mes.localeCompare(b.ano_mes));
        }}

        function agruparPor(records, chave) {{
            const mapa = new Map();
            records.forEach((item) => {{
                const key = item[chave];
                if (!mapa.has(key)) {{
                    mapa.set(key, {{
                        [chave]: key,
                        total_receita: 0,
                        total_despesa: 0,
                        saldo: 0,
                        transacoes: 0,
                        total_movimentado: 0,
                    }});
                }}

                const atual = mapa.get(key);
                const valor = Number(item.valor || 0);
                atual.transacoes += 1;
                atual.total_movimentado += valor;
                if (item.tipo === "receita") {{
                    atual.total_receita += valor;
                }} else {{
                    atual.total_despesa += valor;
                }}
                atual.saldo = atual.total_receita - atual.total_despesa;
            }});

            const resultado = Array.from(mapa.values())
                .map((item) => ({{
                    ...item,
                    ticket_medio: item.transacoes > 0
                        ? Number((item.total_movimentado / item.transacoes).toFixed(2))
                        : 0,
                }}))
                .sort((a, b) => {{
                    const porDespesa = b.total_despesa - a.total_despesa;
                    return porDespesa !== 0 ? porDespesa : b.saldo - a.saldo;
                }});

            const totalDespesa = resultado.reduce((acc, item) => acc + item.total_despesa, 0);
            return resultado.map((item) => ({{
                ...item,
                participacao_despesa: totalDespesa > 0
                    ? Number(((item.total_despesa / totalDespesa) * 100).toFixed(2))
                    : 0,
            }}));
        }}

        function montarEficiencia(mensal, tempos) {{
            const tempoManual = Number(tempos?.manual_min || 2.5);
            const tempoAuto = Number(tempos?.auto_min || 0.3);
            return mensal.map((item) => {{
                const horasManuais = Number(((item.lancamentos * tempoManual) / 60).toFixed(2));
                const horasAuto = Number(((item.lancamentos * tempoAuto) / 60).toFixed(2));
                const horasEconomizadas = Number((horasManuais - horasAuto).toFixed(2));
                return {{
                    ano_mes: item.ano_mes,
                    lancamentos: item.lancamentos,
                    horas_manuais: horasManuais,
                    horas_automatizadas: horasAuto,
                    horas_economizadas: horasEconomizadas,
                    ganho_percentual: horasManuais > 0
                        ? Number(((horasEconomizadas / horasManuais) * 100).toFixed(2))
                        : 0,
                }};
            }});
        }}

        function criarStageVazio() {{
            const vazio = {{ label: "Sem dados", value: 0, format: "int" }};
            const index = {{ id: "sem_dados", label: "Sem dados", format: "int", items: [{{ label: "Sem dados", value: 0 }}] }};
            return {{
                label: "Sem dados",
                title: "Sem dados para os filtros selecionados",
                description: "Ajuste os filtros de mes/categoria/centro de custo para visualizar os indicadores.",
                cards: [vazio],
                indices: [index],
                table: {{ columns: [{{ key: "mensagem", label: "Observacao", format: "text" }}], rows: [{{ mensagem: "Nenhum lancamento encontrado" }}] }},
                highlight: "Os filtros atuais nao retornaram registros.",
            }};
        }}

        function construirStages(records) {{
            if (!records.length) {{
                const vazio = criarStageVazio();
                return {{
                    stageOrder: ["visao_geral", "mensal", "receitas", "categorias", "centros", "eficiencia"],
                    stages: {{
                        visao_geral: vazio,
                        mensal: vazio,
                        receitas: vazio,
                        categorias: vazio,
                        centros: vazio,
                        eficiencia: vazio,
                    }},
                }};
            }}

            const mensal = agruparMensal(records);
            const receitasDetalhadas = agruparReceitaDetalhada(records);
            const categorias = agruparPor(records, "categoria");
            const centros = agruparPor(records, "centro_custo");
            const eficiencia = montarEficiencia(mensal, sourceData.tempos || {{}});
            const metricas = calcularMetricasBase(records);
            const melhorMes = mensal.reduce((prev, atual) => atual.saldo > prev.saldo ? atual : prev, mensal[0]);
            const totalBruta = receitasDetalhadas.reduce((acc, item) => acc + item.receita_bruta, 0);
            const totalDescontos = receitasDetalhadas.reduce((acc, item) => acc + item.descontos, 0);
            const totalLiquida = receitasDetalhadas.reduce((acc, item) => acc + item.receita_liquida, 0);
            const taxaMediaDesconto = totalBruta > 0 ? Number(((totalDescontos / totalBruta) * 100).toFixed(2)) : 0;
            const categoriaTop = categorias[0];
            const centroTop = centros[0];

            const stageOrder = ["visao_geral", "mensal", "receitas", "categorias", "centros", "eficiencia"];
            const stages = {{
                visao_geral: {{
                    label: "Visao geral",
                    title: "Panorama consolidado da operacao",
                    description: "Leitura executiva do recorte filtrado para suporte a decisao.",
                    cards: [
                        {{ label: "Receita total", value: metricas.receita, format: "currency" }},
                        {{ label: "Despesa total", value: metricas.despesa, format: "currency" }},
                        {{ label: "Saldo acumulado", value: metricas.saldo, format: "currency" }},
                        {{ label: "Receita liquida", value: totalLiquida, format: "currency" }},
                        {{ label: "Meses positivos", value: mensal.filter((item) => item.saldo > 0).length, format: "int" }},
                        {{ label: "Taxa media de desconto", value: taxaMediaDesconto, format: "percent" }},
                    ],
                    indices: [
                        {{ id: "saldo", label: "Saldo por mes", format: "currency", items: mensal.map((item) => ({{ label: item.ano_mes, value: item.saldo }})) }},
                        {{ id: "receita", label: "Receita por mes", format: "currency", items: mensal.map((item) => ({{ label: item.ano_mes, value: item.total_receita }})) }},
                        {{ id: "despesa", label: "Despesa por mes", format: "currency", items: mensal.map((item) => ({{ label: item.ano_mes, value: item.total_despesa }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "ano_mes", label: "Mes", format: "text" }},
                            {{ key: "total_receita", label: "Receita", format: "currency" }},
                            {{ key: "total_despesa", label: "Despesa", format: "currency" }},
                            {{ key: "saldo", label: "Saldo", format: "currency" }},
                            {{ key: "margem_percentual", label: "Margem", format: "percent" }},
                        ],
                        rows: mensal,
                    }},
                    highlight: `Melhor mes de saldo: ${{melhorMes.ano_mes}} com ${{formatValue(melhorMes.saldo, "currency")}}.`,
                }},
                mensal: {{
                    label: "Evolucao mensal",
                    title: "Leitura por periodo",
                    description: "Evolucao de receita, despesa, saldo e margem.",
                    cards: [
                        {{ label: "Meses analisados", value: mensal.length, format: "int" }},
                        {{ label: "Media de receita", value: mensal.reduce((acc, item) => acc + item.total_receita, 0) / mensal.length, format: "currency" }},
                        {{ label: "Media de despesa", value: mensal.reduce((acc, item) => acc + item.total_despesa, 0) / mensal.length, format: "currency" }},
                        {{ label: "Melhor margem", value: Math.max(...mensal.map((item) => item.margem_percentual)), format: "percent" }},
                    ],
                    indices: [
                        {{ id: "margem", label: "Margem por mes", format: "percent", items: mensal.map((item) => ({{ label: item.ano_mes, value: item.margem_percentual }})) }},
                        {{ id: "saldo", label: "Saldo por mes", format: "currency", items: mensal.map((item) => ({{ label: item.ano_mes, value: item.saldo }})) }},
                        {{ id: "receita", label: "Receita por mes", format: "currency", items: mensal.map((item) => ({{ label: item.ano_mes, value: item.total_receita }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "ano_mes", label: "Mes", format: "text" }},
                            {{ key: "total_receita", label: "Receita", format: "currency" }},
                            {{ key: "total_despesa", label: "Despesa", format: "currency" }},
                            {{ key: "saldo", label: "Saldo", format: "currency" }},
                            {{ key: "margem_percentual", label: "Margem", format: "percent" }},
                        ],
                        rows: mensal,
                    }},
                    highlight: "Troque o indice para ler cada etapa mensal por perspectiva financeira.",
                }},
                receitas: {{
                    label: "Receitas e descontos",
                    title: "Receita bruta, descontos e receita liquida",
                    description: "Analise financeira focada no comportamento de receita e impacto de descontos comerciais.",
                    cards: [
                        {{ label: "Receita bruta", value: totalBruta, format: "currency" }},
                        {{ label: "Descontos", value: totalDescontos, format: "currency" }},
                        {{ label: "Receita liquida", value: totalLiquida, format: "currency" }},
                        {{ label: "Taxa media de desconto", value: taxaMediaDesconto, format: "percent" }},
                    ],
                    indices: [
                        {{ id: "bruta", label: "Receita bruta por mes", format: "currency", items: receitasDetalhadas.map((item) => ({{ label: item.ano_mes, value: item.receita_bruta }})) }},
                        {{ id: "descontos", label: "Descontos por mes", format: "currency", items: receitasDetalhadas.map((item) => ({{ label: item.ano_mes, value: item.descontos }})) }},
                        {{ id: "liquida", label: "Receita liquida por mes", format: "currency", items: receitasDetalhadas.map((item) => ({{ label: item.ano_mes, value: item.receita_liquida }})) }},
                        {{ id: "taxa", label: "Taxa de desconto", format: "percent", items: receitasDetalhadas.map((item) => ({{ label: item.ano_mes, value: item.taxa_desconto }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "ano_mes", label: "Mes", format: "text" }},
                            {{ key: "receita_bruta", label: "Receita bruta", format: "currency" }},
                            {{ key: "descontos", label: "Descontos", format: "currency" }},
                            {{ key: "receita_liquida", label: "Receita liquida", format: "currency" }},
                            {{ key: "taxa_desconto", label: "Taxa desconto", format: "percent" }},
                        ],
                        rows: receitasDetalhadas,
                    }},
                    highlight: "A leitura de receita liquida evidencia o impacto direto dos descontos sobre o resultado.",
                }},
                categorias: {{
                    label: "Categorias",
                    title: "Pressao e retorno por categoria",
                    description: "Impacto por categoria no recorte filtrado.",
                    cards: [
                        {{ label: "Categorias ativas", value: categorias.length, format: "int" }},
                        {{ label: "Maior gasto", value: categoriaTop.total_despesa, format: "currency" }},
                        {{ label: "Ticket medio", value: categorias.reduce((acc, item) => acc + item.ticket_medio, 0) / categorias.length, format: "currency" }},
                        {{ label: "Participacao topo", value: categoriaTop.participacao_despesa, format: "percent" }},
                    ],
                    indices: [
                        {{ id: "despesa", label: "Despesa por categoria", format: "currency", items: categorias.map((item) => ({{ label: item.categoria, value: item.total_despesa }})) }},
                        {{ id: "saldo", label: "Saldo por categoria", format: "currency", items: categorias.map((item) => ({{ label: item.categoria, value: item.saldo }})) }},
                        {{ id: "participacao", label: "Participacao na despesa", format: "percent", items: categorias.map((item) => ({{ label: item.categoria, value: item.participacao_despesa }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "categoria", label: "Categoria", format: "text" }},
                            {{ key: "total_receita", label: "Receita", format: "currency" }},
                            {{ key: "total_despesa", label: "Despesa", format: "currency" }},
                            {{ key: "saldo", label: "Saldo", format: "currency" }},
                            {{ key: "transacoes", label: "Lancamentos", format: "int" }},
                            {{ key: "ticket_medio", label: "Ticket medio", format: "currency" }},
                        ],
                        rows: categorias,
                    }},
                    highlight: `Categoria com maior pressao de custo: ${{categoriaTop.categoria}} (${{formatValue(categoriaTop.total_despesa, "currency")}}).`,
                }},
                centros: {{
                    label: "Centros de custo",
                    title: "Carga operacional por centro",
                    description: "Distribuicao de consumo e retorno por centro de custo.",
                    cards: [
                        {{ label: "Centros ativos", value: centros.length, format: "int" }},
                        {{ label: "Maior centro de gasto", value: centroTop.total_despesa, format: "currency" }},
                        {{ label: "Saldo medio", value: centros.reduce((acc, item) => acc + item.saldo, 0) / centros.length, format: "currency" }},
                        {{ label: "Ticket medio", value: centros.reduce((acc, item) => acc + item.ticket_medio, 0) / centros.length, format: "currency" }},
                    ],
                    indices: [
                        {{ id: "despesa", label: "Despesa por centro", format: "currency", items: centros.map((item) => ({{ label: item.centro_custo, value: item.total_despesa }})) }},
                        {{ id: "receita", label: "Receita por centro", format: "currency", items: centros.map((item) => ({{ label: item.centro_custo, value: item.total_receita }})) }},
                        {{ id: "saldo", label: "Saldo por centro", format: "currency", items: centros.map((item) => ({{ label: item.centro_custo, value: item.saldo }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "centro_custo", label: "Centro", format: "text" }},
                            {{ key: "total_receita", label: "Receita", format: "currency" }},
                            {{ key: "total_despesa", label: "Despesa", format: "currency" }},
                            {{ key: "saldo", label: "Saldo", format: "currency" }},
                            {{ key: "transacoes", label: "Lancamentos", format: "int" }},
                            {{ key: "ticket_medio", label: "Ticket medio", format: "currency" }},
                        ],
                        rows: centros,
                    }},
                    highlight: `Centro com maior volume de despesa: ${{centroTop.centro_custo}} (${{formatValue(centroTop.total_despesa, "currency")}}).`,
                }},
                eficiencia: {{
                    label: "Eficiencia",
                    title: "Impacto operacional da automacao",
                    description: "Producao por mes em horas manuais vs automacao.",
                    cards: [
                        {{ label: "Lancamentos tratados", value: records.length, format: "int" }},
                        {{ label: "Horas manuais", value: eficiencia.reduce((acc, item) => acc + item.horas_manuais, 0), format: "hours" }},
                        {{ label: "Horas automatizadas", value: eficiencia.reduce((acc, item) => acc + item.horas_automatizadas, 0), format: "hours" }},
                        {{ label: "Horas economizadas", value: eficiencia.reduce((acc, item) => acc + item.horas_economizadas, 0), format: "hours" }},
                        {{ label: "Reducao de tempo", value: Number(sourceData.tempos?.manual_min || 2.5) > 0 ? Number((((Number(sourceData.tempos?.manual_min || 2.5) - Number(sourceData.tempos?.auto_min || 0.3)) / Number(sourceData.tempos?.manual_min || 2.5)) * 100).toFixed(2)) : 0, format: "percent" }},
                    ],
                    indices: [
                        {{ id: "economia", label: "Horas economizadas por mes", format: "hours", items: eficiencia.map((item) => ({{ label: item.ano_mes, value: item.horas_economizadas }})) }},
                        {{ id: "volume", label: "Volume de lancamentos", format: "int", items: eficiencia.map((item) => ({{ label: item.ano_mes, value: item.lancamentos }})) }},
                        {{ id: "ganho", label: "Ganho percentual por mes", format: "percent", items: eficiencia.map((item) => ({{ label: item.ano_mes, value: item.ganho_percentual }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "ano_mes", label: "Mes", format: "text" }},
                            {{ key: "lancamentos", label: "Lancamentos", format: "int" }},
                            {{ key: "horas_manuais", label: "Horas manuais", format: "hours" }},
                            {{ key: "horas_automatizadas", label: "Horas auto", format: "hours" }},
                            {{ key: "horas_economizadas", label: "Horas economizadas", format: "hours" }},
                            {{ key: "ganho_percentual", label: "Ganho", format: "percent" }},
                        ],
                        rows: eficiencia,
                    }},
                    highlight: "A produtividade fica visivel ao combinar recortes de mes, categoria e centro de custo.",
                }},
            }};

            return {{ stageOrder, stages }};
        }}

        function preencherSelect(selectEl, valores, valorAtual, rotuloTodos) {{
            const opcoes = [rotuloTodos, ...valores];
            selectEl.innerHTML = opcoes
                .map((valor, index) => `<option value="${{index === 0 ? "todos" : valor}}">${{valor}}</option>`)
                .join("");
            selectEl.value = valorAtual;
        }}

        function popularFiltros() {{
            const meses = Array.from(new Set(rawRecords.map((item) => item.ano_mes))).sort();
            const categorias = Array.from(new Set(rawRecords.map((item) => item.categoria))).sort();
            const centros = Array.from(new Set(rawRecords.map((item) => item.centro_custo))).sort();

            preencherSelect(mesFilter, meses, state.mes, "Todos os meses");
            preencherSelect(categoriaFilter, categorias, state.categoria, "Todas as categorias");
            preencherSelect(centroFilter, centros, state.centro, "Todos os centros");
        }}

        function filtrarRegistros() {{
            return rawRecords.filter((item) => (
                (state.mes === "todos" || item.ano_mes === state.mes)
                && (state.categoria === "todos" || item.categoria === state.categoria)
                && (state.centro === "todos" || item.centro_custo === state.centro)
            ));
        }}

        function renderStageOptions() {{
            stageSelect.innerHTML = "";
            stageNav.innerHTML = "";

            currentStages.stageOrder.forEach((stageId) => {{
                const stage = currentStages.stages[stageId];

                const option = document.createElement("option");
                option.value = stageId;
                option.textContent = stage.label;
                stageSelect.appendChild(option);

                const button = document.createElement("button");
                button.type = "button";
                button.className = `stage-button${{stageId === state.stageId ? " active" : ""}}`;
                button.textContent = stage.label;
                button.addEventListener("click", () => {{
                    state.stageId = stageId;
                    syncStage();
                }});
                stageNav.appendChild(button);
            }});

            stageSelect.value = state.stageId;
        }}

        function renderIndexOptions(stage) {{
            indexSelect.innerHTML = "";
            stage.indices.forEach((indexItem, position) => {{
                const option = document.createElement("option");
                option.value = indexItem.id;
                option.textContent = indexItem.label;
                indexSelect.appendChild(option);

                if (!state.indexId && position === 0) {{
                    state.indexId = indexItem.id;
                }}
            }});

            if (!stage.indices.some((item) => item.id === state.indexId)) {{
                state.indexId = stage.indices[0].id;
            }}
            indexSelect.value = state.indexId;
        }}

        function renderCards(stage) {{
            cardsGrid.innerHTML = "";
            stage.cards.forEach((card) => {{
                const article = document.createElement("article");
                article.className = "kpi";
                article.innerHTML = `<small>${{card.label}}</small><strong>${{formatValue(card.value, card.format)}}</strong>`;
                cardsGrid.appendChild(article);
            }});
        }}

        function renderCanvas(selectedIndex) {{
            const ratio = window.devicePixelRatio || 1;
            const width = Math.max(barsCanvas.clientWidth, 320);
            barsCanvas.width = Math.floor(width * ratio);
            barsCanvas.height = Math.floor(chartHeight * ratio);
            barsCanvas.style.height = `${{chartHeight}}px`;

            const ctx = barsCanvas.getContext("2d");
            ctx.scale(ratio, ratio);
            ctx.clearRect(0, 0, width, chartHeight);

            const items = selectedIndex.items.slice(0, 12);
            const values = items.map((item) => Number(item.value) || 0);
            const maxAbs = Math.max(...values.map((value) => Math.abs(value)), 1);

            const margin = {{ top: 24, right: 16, bottom: 56, left: 56 }};
            const chartW = width - margin.left - margin.right;
            const chartH = chartHeight - margin.top - margin.bottom;
            const zeroY = margin.top + chartH / 2;
            const barWidth = chartW / Math.max(items.length, 1) * 0.62;

            ctx.strokeStyle = "#c8baa9";
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(margin.left, zeroY);
            ctx.lineTo(width - margin.right, zeroY);
            ctx.stroke();

            items.forEach((item, index) => {{
                const x = margin.left + (index * chartW / items.length) + ((chartW / items.length) - barWidth) / 2;
                const value = Number(item.value) || 0;
                const size = Math.abs(value / maxAbs) * (chartH / 2 - 6);
                const y = value >= 0 ? zeroY - size : zeroY;

                ctx.fillStyle = value >= 0 ? "#17313e" : "#9a3412";
                ctx.fillRect(x, y, barWidth, Math.max(size, 2));

                ctx.fillStyle = "#6e6258";
                ctx.font = "12px Aptos, Trebuchet MS, sans-serif";
                ctx.textAlign = "center";
                ctx.fillText(item.label.slice(0, 10), x + barWidth / 2, chartHeight - 24);

                ctx.fillStyle = "#1b1b1b";
                ctx.font = "11px Aptos, Trebuchet MS, sans-serif";
                const valor = formatValue(item.value, selectedIndex.format).replace("R$\u00a0", "");
                ctx.fillText(valor, x + barWidth / 2, value >= 0 ? y - 8 : y + Math.max(size, 2) + 14);
            }});
        }}

        function renderTrendSvg(selectedIndex) {{
            const items = selectedIndex.items.slice(0, 20);
            if (!items.length) {{
                trendSvg.innerHTML = "";
                return;
            }}

            const width = 540;
            const height = 120;
            const padding = 14;
            const values = items.map((item) => Number(item.value) || 0);
            const min = Math.min(...values);
            const max = Math.max(...values);
            const span = Math.max(max - min, 1);
            const pontos = items.map((item, index) => {{
                const x = padding + (index * (width - padding * 2) / Math.max(items.length - 1, 1));
                const y = height - padding - ((Number(item.value || 0) - min) / span) * (height - padding * 2);
                return `${{x}},${{y}}`;
            }}).join(" ");

            trendSvg.innerHTML = `
                <polyline fill="none" stroke="#e9d7c2" stroke-width="2" points="${{padding}},${{height - padding}} ${{width - padding}},${{height - padding}}"></polyline>
                <polyline fill="none" stroke="#c46a2f" stroke-width="3" points="${{pontos}}"></polyline>
            `;
        }}

        function renderCharts(stage, selectedIndex) {{
            chartTitle.textContent = selectedIndex.label;
            chartCaption.textContent = "Grafico em canvas com linha de tendencia em SVG no recorte atual.";
            renderCanvas(selectedIndex);
            renderTrendSvg(selectedIndex);

            highlight.textContent = stage.highlight;
        }}

        function renderTable(stage) {{
            tableHead.innerHTML = "";
            tableBody.innerHTML = "";

            const headerRow = document.createElement("tr");
            stage.table.columns.forEach((column) => {{
                const th = document.createElement("th");
                th.textContent = column.label;
                headerRow.appendChild(th);
            }});
            tableHead.appendChild(headerRow);

            stage.table.rows.forEach((row) => {{
                const tr = document.createElement("tr");
                stage.table.columns.forEach((column) => {{
                    const td = document.createElement("td");
                    td.textContent = formatValue(row[column.key], column.format);
                    tr.appendChild(td);
                }});
                tableBody.appendChild(tr);
            }});
        }}

        function syncStage(keepIndex = false) {{
            renderStageOptions();
            const stage = currentStages.stages[state.stageId];
            stageTitle.textContent = stage.title;
            stageDescription.textContent = stage.description;
            if (!keepIndex) {{
                state.indexId = null;
            }}
            renderIndexOptions(stage);
            renderCards(stage);
            renderTable(stage);
            renderCharts(stage, stage.indices.find((item) => item.id === state.indexId));
        }}

        function atualizarDashboard() {{
            const filtrado = filtrarRegistros();
            datasetStatus.textContent = `Lancamentos no recorte atual: ${{filtrado.length}} de ${{rawRecords.length}}.`;
            currentStages = construirStages(filtrado);
            if (!currentStages.stages[state.stageId]) {{
                state.stageId = currentStages.stageOrder[0];
                state.indexId = null;
            }}
            syncStage(false);
        }}

        stageSelect.addEventListener("change", (event) => {{
            state.stageId = event.target.value;
            syncStage(false);
        }});

        indexSelect.addEventListener("change", (event) => {{
            state.indexId = event.target.value;
            const stage = currentStages.stages[state.stageId];
            renderCharts(stage, stage.indices.find((item) => item.id === state.indexId));
        }});

        mesFilter.addEventListener("change", (event) => {{
            state.mes = event.target.value;
            atualizarDashboard();
        }});

        categoriaFilter.addEventListener("change", (event) => {{
            state.categoria = event.target.value;
            atualizarDashboard();
        }});

        centroFilter.addEventListener("change", (event) => {{
            state.centro = event.target.value;
            atualizarDashboard();
        }});

        window.addEventListener("resize", () => {{
            if (!currentStages) {{
                return;
            }}
            const stage = currentStages.stages[state.stageId];
            const selectedIndex = stage.indices.find((item) => item.id === state.indexId) || stage.indices[0];
            renderCharts(stage, selectedIndex);
        }});

        async function iniciar() {{
            try {{
                const resposta = await fetch(DATA_URL, {{ cache: "no-store" }});
                if (!resposta.ok) {{
                    throw new Error(`Falha ao carregar ${{DATA_URL}}`);
                }}
                sourceData = await resposta.json();
                rawRecords = Array.isArray(sourceData.records) ? sourceData.records : [];
                popularFiltros();
                atualizarDashboard();
            }} catch (erro) {{
                datasetStatus.textContent = "Nao foi possivel carregar o JSON externo. Execute o script novamente ou abra via servidor HTTP.";
                console.error(erro);
            }}
        }}

        iniciar();
    </script>
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
    exemplo = pd.DataFrame(EXEMPLO_LANCAMENTOS, columns=COLUNAS_PADRAO)
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
        base,
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