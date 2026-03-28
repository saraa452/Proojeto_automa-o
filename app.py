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
import importlib
import json
import logging
import math
import shutil
from datetime import date
from typing import Any, cast
from pathlib import Path

import numpy as np
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


def adicionar_datas_fluxo_caixa(df: pd.DataFrame) -> pd.DataFrame:
    """Gera datas de recebimento/pagamento para analise de liquidez."""
    base = df.copy()

    prazo_recebimento = {
        "assinaturas": 5,
        "projetos": 30,
        "servicos": 20,
        "vendas": 25,
    }
    prazo_pagamento = {
        "pessoal": 2,
        "fornecedores": 30,
        "marketing": 15,
        "tecnologia": 20,
        "infraestrutura": 10,
        "operacoes": 18,
        "descontos_comerciais": 0,
    }

    def _prazo(row: pd.Series) -> int:
        categoria = str(row["categoria"]).lower()
        if row["tipo"] == "receita":
            return int(prazo_recebimento.get(categoria, 20))
        return int(prazo_pagamento.get(categoria, 15))

    base["prazo_dias"] = base.apply(_prazo, axis=1)
    base["data_recebimento"] = pd.NaT
    base["data_pagamento"] = pd.NaT

    mask_receita = base["tipo"] == "receita"
    mask_despesa = base["tipo"] == "despesa"

    base.loc[mask_receita, "data_recebimento"] = (
        base.loc[mask_receita, "data"] + pd.to_timedelta(base.loc[mask_receita, "prazo_dias"], unit="D")
    )
    base.loc[mask_despesa, "data_pagamento"] = (
        base.loc[mask_despesa, "data"] + pd.to_timedelta(base.loc[mask_despesa, "prazo_dias"], unit="D")
    )

    base["data_caixa"] = base["data_recebimento"].fillna(base["data_pagamento"])
    base["ano_mes_caixa"] = base["data_caixa"].dt.to_period("M").astype(str)
    base["valor_caixa"] = np.where(base["tipo"] == "receita", base["valor"], -base["valor"])
    return base


def gerar_sazonalidade(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula padrao sazonal por mes calendario."""
    mensal = gerar_resumo_mensal(df)
    mensal["ano"] = mensal["ano_mes"].str.slice(0, 4).astype(int)
    mensal["mes"] = mensal["ano_mes"].str.slice(5, 7).astype(int)

    sazonal = (
        mensal.groupby("mes", as_index=False)
        .agg(
            media_receita=("total_receita", "mean"),
            media_despesa=("total_despesa", "mean"),
            media_saldo=("saldo", "mean"),
            pico_receita=("total_receita", "max"),
            pico_despesa=("total_despesa", "max"),
        )
        .sort_values("mes")
        .reset_index(drop=True)
    )
    sazonal["mes_nome"] = sazonal["mes"].map(
        {
            1: "jan",
            2: "fev",
            3: "mar",
            4: "abr",
            5: "mai",
            6: "jun",
            7: "jul",
            8: "ago",
            9: "set",
            10: "out",
            11: "nov",
            12: "dez",
        }
    )
    return sazonal[["mes", "mes_nome", "media_receita", "media_despesa", "media_saldo", "pico_receita", "pico_despesa"]]


def _forecast_series_sarima_ou_fallback(
    serie: pd.Series,
    periods: int = 6,
) -> tuple[pd.Series, str]:
    """Preve serie mensal com SARIMA quando disponivel e fallback linear+sazonal."""
    serie = serie.astype(float)
    serie.index = pd.to_datetime(serie.index)
    serie = serie.asfreq("MS")

    try:
        sarimax_module = importlib.import_module("statsmodels.tsa.statespace.sarimax")
        SARIMAX = sarimax_module.SARIMAX
        seasonal_order = (1, 1, 1, 12) if len(serie) >= 24 else (0, 0, 0, 0)
        modelo = SARIMAX(
            serie,
            order=(1, 1, 1),
            seasonal_order=seasonal_order,
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        ajuste = modelo.fit(disp=False)
        pred = ajuste.forecast(steps=periods)
        pred = pd.Series(np.maximum(pred.values, 0.0), index=pred.index)
        return pred, "SARIMA"
    except Exception:
        pass

    # Fallback deterministico para ambientes sem statsmodels.
    idx_futuro = pd.date_range(start=serie.index.max() + pd.offsets.MonthBegin(1), periods=periods, freq="MS")
    if len(serie) >= 2:
        tendencia = (serie.iloc[-1] - serie.iloc[0]) / max(len(serie) - 1, 1)
    else:
        tendencia = 0.0

    indice_dt = pd.DatetimeIndex(serie.index)
    sazonal = serie.groupby(indice_dt.month).mean()
    base = float(serie.iloc[-1])
    valores = []
    for passo, dt_ref in enumerate(idx_futuro, start=1):
        componente_sazonal = float(sazonal.get(dt_ref.month, serie.mean()))
        valor = max(base + tendencia * passo, 0.0)
        valor = (valor * 0.6) + (componente_sazonal * 0.4)
        valores.append(valor)
    pred = pd.Series(valores, index=idx_futuro)
    return pred, "FallbackLinearSazonal"


def gerar_projecao_financeira(df: pd.DataFrame, periods: int = 6) -> tuple[pd.DataFrame, str]:
    """Gera projecao mensal de receitas e despesas para os proximos meses."""
    mensal = gerar_resumo_mensal(df)
    historico = mensal.copy()
    historico["data_ref"] = pd.to_datetime(historico["ano_mes"] + "-01")

    serie_receita = historico.set_index("data_ref")["total_receita"]
    serie_despesa = historico.set_index("data_ref")["total_despesa"]

    prev_receita, modelo_receita = _forecast_series_sarima_ou_fallback(serie_receita, periods=periods)
    prev_despesa, modelo_despesa = _forecast_series_sarima_ou_fallback(serie_despesa, periods=periods)
    modelo = modelo_receita if modelo_receita == modelo_despesa else f"{modelo_receita}+{modelo_despesa}"

    previsao = pd.DataFrame(
        {
            "data_ref": prev_receita.index,
            "total_receita": prev_receita.values,
            "total_despesa": prev_despesa.values,
            "tipo": "projecao",
        }
    )
    previsao["saldo"] = previsao["total_receita"] - previsao["total_despesa"]
    previsao["ano_mes"] = previsao["data_ref"].dt.to_period("M").astype(str)

    historico_view = historico[["data_ref", "ano_mes", "total_receita", "total_despesa", "saldo"]].copy()
    historico_view["tipo"] = "historico"

    combinado = pd.concat([historico_view, previsao], ignore_index=True)
    return combinado[["ano_mes", "tipo", "total_receita", "total_despesa", "saldo"]], modelo


def analisar_descontos(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """Avalia correlacao dos descontos com receita subsequente e payback por categoria."""
    descontos = (
        df[(df["tipo"] == "despesa") & (df["categoria"].str.contains("desconto", case=False, na=False))]
        .groupby("ano_mes", as_index=False)
        .agg(descontos=("valor", "sum"))
    )
    receita = (
        df[df["tipo"] == "receita"]
        .groupby("ano_mes", as_index=False)
        .agg(receita_mes=("valor", "sum"))
    )

    base_mes = receita.merge(descontos, on="ano_mes", how="left").fillna({"descontos": 0.0})
    base_mes = base_mes.sort_values("ano_mes").reset_index(drop=True)
    base_mes["receita_subsequente"] = base_mes["receita_mes"].shift(-1)
    base_mes["delta_receita_subsequente"] = base_mes["receita_subsequente"] - base_mes["receita_mes"]
    base_mes["payback"] = base_mes.apply(
        lambda row: (row["delta_receita_subsequente"] / row["descontos"]) if row["descontos"] > 0 else np.nan,
        axis=1,
    )

    correlacao = 0.0
    amostra_corr = base_mes.dropna(subset=["receita_subsequente"])
    if len(amostra_corr) >= 3 and amostra_corr["descontos"].std() > 0 and amostra_corr["receita_subsequente"].std() > 0:
        correlacao = float(amostra_corr["descontos"].corr(amostra_corr["receita_subsequente"]))

    receita_cat = (
        df[df["tipo"] == "receita"]
        .groupby(["ano_mes", "categoria"], as_index=False)
        .agg(receita_categoria=("valor", "sum"))
    )
    total_receita_mes = (
        receita_cat.groupby("ano_mes", as_index=False)
        .agg(receita_total_mes=("receita_categoria", "sum"))
    )
    receita_cat = receita_cat.merge(total_receita_mes, on="ano_mes", how="left")
    receita_cat = receita_cat.merge(descontos, on="ano_mes", how="left").fillna({"descontos": 0.0})
    receita_cat["desconto_alocado"] = receita_cat.apply(
        lambda row: row["descontos"] * (row["receita_categoria"] / row["receita_total_mes"]) if row["receita_total_mes"] > 0 else 0.0,
        axis=1,
    )

    receita_cat = receita_cat.sort_values(["categoria", "ano_mes"]).reset_index(drop=True)
    receita_cat["receita_categoria_subsequente"] = receita_cat.groupby("categoria")["receita_categoria"].shift(-1)
    receita_cat["delta_receita_subsequente"] = (
        receita_cat["receita_categoria_subsequente"] - receita_cat["receita_categoria"]
    )

    payback_categoria = (
        receita_cat.groupby("categoria", as_index=False)
        .agg(
            desconto_alocado=("desconto_alocado", "sum"),
            delta_receita_subsequente=("delta_receita_subsequente", "sum"),
        )
        .sort_values("desconto_alocado", ascending=False)
        .reset_index(drop=True)
    )
    payback_categoria["payback"] = payback_categoria.apply(
        lambda row: (row["delta_receita_subsequente"] / row["desconto_alocado"]) if row["desconto_alocado"] > 0 else np.nan,
        axis=1,
    )

    return base_mes, payback_categoria, correlacao


def analisar_rentabilidade_centros(df: pd.DataFrame) -> pd.DataFrame:
    """Aloca despesas compartilhadas (RH/ADM/TI) para centros de receita."""
    resumo = _agrupar_financeiro(df, ["centro_custo"])
    suporte = {"rh", "adm", "ti"}

    receita_centros = resumo[resumo["total_receita"] > 0][["centro_custo", "total_receita", "total_despesa"]].copy()
    if receita_centros.empty:
        receita_centros = resumo[["centro_custo", "total_receita", "total_despesa"]].copy()

    despesa_compartilhada = float(
        df[(df["tipo"] == "despesa") & (df["centro_custo"].isin(suporte))]["valor"].sum()
    )
    total_receita = float(receita_centros["total_receita"].sum())
    receita_centros["peso_receita"] = receita_centros["total_receita"].apply(
        lambda valor: (valor / total_receita) if total_receita > 0 else 0.0
    )
    receita_centros["despesa_alocada_suporte"] = receita_centros["peso_receita"] * despesa_compartilhada
    receita_centros["despesa_total_alocada"] = (
        receita_centros["total_despesa"] + receita_centros["despesa_alocada_suporte"]
    )
    receita_centros["margem_liquida"] = (
        receita_centros["total_receita"] - receita_centros["despesa_total_alocada"]
    )
    receita_centros["margem_percentual"] = receita_centros.apply(
        lambda row: (row["margem_liquida"] / row["total_receita"] * 100) if row["total_receita"] > 0 else 0.0,
        axis=1,
    )
    return receita_centros.sort_values("margem_liquida", ascending=False).reset_index(drop=True)


def analisar_mix_receita(df: pd.DataFrame) -> pd.DataFrame:
    """Acompanha mix entre receita recorrente, projetos e demais receitas."""
    receitas = df[df["tipo"] == "receita"].copy()
    receitas["mix_grupo"] = "outras_receitas"
    receitas.loc[receitas["categoria"].eq("assinaturas"), "mix_grupo"] = "recorrentes_assinaturas"
    receitas.loc[receitas["categoria"].eq("projetos"), "mix_grupo"] = "projetos_pontuais"

    mix = (
        receitas.groupby(["ano_mes", "mix_grupo"], as_index=False)["valor"]
        .sum()
        .pivot(index="ano_mes", columns="mix_grupo", values="valor")
        .fillna(0)
        .reset_index()
    )

    for coluna in ["recorrentes_assinaturas", "projetos_pontuais", "outras_receitas"]:
        if coluna not in mix.columns:
            mix[coluna] = 0.0

    mix["receita_total"] = (
        mix["recorrentes_assinaturas"] + mix["projetos_pontuais"] + mix["outras_receitas"]
    )
    mix["share_recorrente"] = mix.apply(
        lambda row: (row["recorrentes_assinaturas"] / row["receita_total"] * 100) if row["receita_total"] > 0 else 0.0,
        axis=1,
    )
    mix["share_projetos"] = mix.apply(
        lambda row: (row["projetos_pontuais"] / row["receita_total"] * 100) if row["receita_total"] > 0 else 0.0,
        axis=1,
    )
    return mix.sort_values("ano_mes").reset_index(drop=True)


def analisar_produtividade_pessoal(df: pd.DataFrame) -> pd.DataFrame:
    """Relaciona despesas de pessoal com receita de projetos."""
    pessoal = (
        df[(df["tipo"] == "despesa") & (df["categoria"] == "pessoal")]
        .groupby("ano_mes", as_index=False)
        .agg(despesa_pessoal=("valor", "sum"))
    )
    projetos = (
        df[(df["tipo"] == "receita") & (df["categoria"] == "projetos")]
        .groupby("ano_mes", as_index=False)
        .agg(receita_projetos=("valor", "sum"))
    )
    qtd_projetos = (
        df[(df["tipo"] == "receita") & (df["categoria"] == "projetos")]
        .groupby("ano_mes", as_index=False)
        .size()
        .rename(columns={"size": "qtd_projetos"})
    )

    produtividade = (
        projetos.merge(pessoal, on="ano_mes", how="outer")
        .merge(qtd_projetos, on="ano_mes", how="left")
        .fillna({"receita_projetos": 0.0, "despesa_pessoal": 0.0, "qtd_projetos": 0})
        .sort_values("ano_mes")
        .reset_index(drop=True)
    )
    produtividade["produtividade_receita_por_pessoal"] = produtividade.apply(
        lambda row: (row["receita_projetos"] / row["despesa_pessoal"]) if row["despesa_pessoal"] > 0 else np.nan,
        axis=1,
    )
    produtividade["custo_pessoal_por_projeto"] = produtividade.apply(
        lambda row: (row["despesa_pessoal"] / row["qtd_projetos"]) if row["qtd_projetos"] > 0 else np.nan,
        axis=1,
    )
    return produtividade


def detectar_anomalias(df: pd.DataFrame) -> pd.DataFrame:
    """Identifica lancamentos atipicos por tipo com base em z-score robusto."""
    base = df.copy()
    base["valor_abs"] = base["valor"].abs()

    anomalias: list[pd.DataFrame] = []
    for tipo in ["receita", "despesa"]:
        grupo = base[base["tipo"] == tipo].copy()
        if grupo.empty:
            continue
        mediana = float(grupo["valor_abs"].median())
        mad = float((grupo["valor_abs"] - mediana).abs().median())
        if mad == 0:
            continue
        grupo["z_robusto"] = 0.6745 * (grupo["valor_abs"] - mediana) / mad
        grupo = grupo[grupo["z_robusto"].abs() >= 2.8]
        anomalias.append(grupo)

    if not anomalias:
        return pd.DataFrame(
            columns=["data", "ano_mes", "descricao", "categoria", "tipo", "valor", "centro_custo", "z_robusto"]
        )

    consolidado = pd.concat(anomalias, ignore_index=True)
    return consolidado[
        ["data", "ano_mes", "descricao", "categoria", "tipo", "valor", "centro_custo", "z_robusto"]
    ].sort_values("z_robusto", ascending=False)


def gerar_benchmarking_yoy(df: pd.DataFrame) -> pd.DataFrame:
    """Compara meses equivalentes entre anos para crescimento organico."""
    mensal = gerar_resumo_mensal(df)
    mensal["ano"] = mensal["ano_mes"].str.slice(0, 4).astype(int)
    mensal["mes"] = mensal["ano_mes"].str.slice(5, 7).astype(int)

    atual = mensal.copy()
    anterior = mensal.copy()
    anterior["ano"] = anterior["ano"] + 1

    comparativo = atual.merge(
        anterior[["ano", "mes", "total_receita", "total_despesa", "saldo"]],
        on=["ano", "mes"],
        how="left",
        suffixes=("_atual", "_ano_anterior"),
    )
    comparativo = comparativo.dropna(subset=["total_receita_ano_anterior"]).reset_index(drop=True)

    for campo in ["total_receita", "total_despesa", "saldo"]:
        comparativo[f"crescimento_{campo}"] = comparativo.apply(
            lambda row: (
                (row[f"{campo}_atual"] - row[f"{campo}_ano_anterior"])
                / row[f"{campo}_ano_anterior"]
                * 100
            )
            if pd.notna(row[f"{campo}_ano_anterior"]) and row[f"{campo}_ano_anterior"] != 0
            else np.nan,
            axis=1,
        )

    comparativo["ano_mes"] = comparativo["ano"].astype(str) + "-" + comparativo["mes"].astype(str).str.zfill(2)
    return comparativo[
        [
            "ano_mes",
            "total_receita_atual",
            "total_receita_ano_anterior",
            "crescimento_total_receita",
            "total_despesa_atual",
            "total_despesa_ano_anterior",
            "crescimento_total_despesa",
            "saldo_atual",
            "saldo_ano_anterior",
            "crescimento_saldo",
        ]
    ].sort_values("ano_mes")


def analisar_fluxo_caixa(df_com_fluxo: pd.DataFrame) -> pd.DataFrame:
    """Calcula visao de liquidez e necessidade de capital de giro."""
    fluxo = (
        df_com_fluxo.groupby(["ano_mes_caixa", "tipo"], as_index=False)["valor"]
        .sum()
        .pivot(index="ano_mes_caixa", columns="tipo", values="valor")
        .fillna(0)
        .reset_index()
    )

    if "receita" not in fluxo.columns:
        fluxo["receita"] = 0.0
    if "despesa" not in fluxo.columns:
        fluxo["despesa"] = 0.0

    fluxo = fluxo.rename(columns={"ano_mes_caixa": "ano_mes", "receita": "entradas", "despesa": "saidas"})
    fluxo["saldo_caixa"] = fluxo["entradas"] - fluxo["saidas"]
    fluxo["saldo_acumulado"] = fluxo["saldo_caixa"].cumsum()

    prazo_recebimento = df_com_fluxo[df_com_fluxo["tipo"] == "receita"]["prazo_dias"].mean()
    prazo_pagamento = df_com_fluxo[df_com_fluxo["tipo"] == "despesa"]["prazo_dias"].mean()
    ciclo_caixa = float((prazo_recebimento or 0.0) - (prazo_pagamento or 0.0))
    fluxo["ciclo_caixa_dias"] = round(ciclo_caixa, 2)
    fluxo["necessidade_capital_giro"] = fluxo["saldo_acumulado"].apply(lambda v: abs(v) if v < 0 else 0.0)
    return fluxo.sort_values("ano_mes").reset_index(drop=True)


def gerar_analises_avancadas(df: pd.DataFrame) -> dict[str, Any]:
    """Orquestra todas as analises adicionais para o dashboard e relatorios."""
    base_fluxo = adicionar_datas_fluxo_caixa(df)
    sazonalidade = gerar_sazonalidade(df)
    projecao, modelo_previsao = gerar_projecao_financeira(df)
    descontos_mensal, payback_categoria, correlacao_descontos = analisar_descontos(df)
    rentabilidade = analisar_rentabilidade_centros(df)
    mix_receita = analisar_mix_receita(df)
    produtividade = analisar_produtividade_pessoal(df)
    anomalias = detectar_anomalias(df)
    benchmarking = gerar_benchmarking_yoy(df)
    liquidez = analisar_fluxo_caixa(base_fluxo)

    pico_receita = sazonalidade.sort_values("media_receita", ascending=False).head(1)
    pico_despesa = sazonalidade.sort_values("media_despesa", ascending=False).head(1)

    insights = {
        "modelo_previsao": modelo_previsao,
        "correlacao_descontos_receita_subsequente": round(correlacao_descontos, 4),
        "pico_receita_mes": pico_receita.iloc[0]["mes_nome"] if not pico_receita.empty else "n/a",
        "pico_despesa_mes": pico_despesa.iloc[0]["mes_nome"] if not pico_despesa.empty else "n/a",
        "descontos_total": float(descontos_mensal["descontos"].sum()) if not descontos_mensal.empty else 0.0,
        "despesa_pessoal_total": float(df[df["categoria"].eq("pessoal")]["valor"].sum()),
    }

    return {
        "base_fluxo": base_fluxo,
        "sazonalidade": sazonalidade,
        "projecao": projecao,
        "descontos_mensal": descontos_mensal,
        "payback_categoria": payback_categoria,
        "rentabilidade": rentabilidade,
        "mix_receita": mix_receita,
        "produtividade": produtividade,
        "anomalias": anomalias,
        "benchmarking": benchmarking,
        "liquidez": liquidez,
        "insights": insights,
    }


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


def _sanitize_nan(obj: Any) -> Any:
    """Substitui float NaN/Inf por None recursivamente para gerar JSON valido."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_nan(v) for v in obj]
    return obj


def montar_payload_site(
    base: pd.DataFrame,
    mensal: pd.DataFrame,
    categoria: pd.DataFrame,
    indicadores: pd.DataFrame,
    analises: dict[str, Any],
) -> dict[str, object]:
    """Monta os dados-base serializados para o dashboard interativo."""
    registros = base.copy()
    meses_recentes = sorted(registros["ano_mes"].dropna().unique())[-12:]
    registros = registros[registros["ano_mes"].isin(meses_recentes)].copy()
    registros["data"] = registros["data"].dt.strftime("%Y-%m-%d")
    registros["valor"] = registros["valor"].astype(float).round(2)
    registros["valor_assinado"] = registros["valor_assinado"].astype(float).round(2)

    if "data_recebimento" in registros.columns:
        registros["data_recebimento"] = pd.to_datetime(registros["data_recebimento"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "data_pagamento" in registros.columns:
        registros["data_pagamento"] = pd.to_datetime(registros["data_pagamento"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "data_caixa" in registros.columns:
        registros["data_caixa"] = pd.to_datetime(registros["data_caixa"], errors="coerce").dt.strftime("%Y-%m-%d")

    def _to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
        if df.empty:
            return []
        safe = df.copy()
        for col in safe.columns:
            if pd.api.types.is_datetime64_any_dtype(safe[col]):
                safe[col] = safe[col].dt.strftime("%Y-%m-%d")
        registros_json = safe.replace({np.nan: None}).to_dict(orient="records")
        return cast(list[dict[str, Any]], registros_json)

    return {
        "generatedAt": date.today().isoformat(),
        "tempos": {
            "manual_min": TEMPO_MANUAL_MIN,
            "auto_min": TEMPO_AUTO_MIN,
        },
        "insights": analises.get("insights", {}),
        "analytics": {
            "projecao": _to_records(analises.get("projecao", pd.DataFrame()).tail(18)),
            "descontos_mensal": _to_records(analises.get("descontos_mensal", pd.DataFrame()).tail(12)),
            "rentabilidade": _to_records(analises.get("rentabilidade", pd.DataFrame()).head(8)),
            "liquidez": _to_records(analises.get("liquidez", pd.DataFrame()).tail(12)),
        },
        "records": _to_records(registros[
            [
                "data",
                "ano_mes",
                "descricao",
                "categoria",
                "tipo",
                "valor",
                "centro_custo",
                "valor_assinado",
                "prazo_dias",
                "data_recebimento",
                "data_pagamento",
                "data_caixa",
                "ano_mes_caixa",
                "valor_caixa",
            ]
        ]),
    }
    return _sanitize_nan(payload)


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
    analises: dict[str, Any],
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
    insights = analises.get("insights", {})
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
        "## 4. Sazonalidade e Projecao",
        f"- Modelo de previsao utilizado: {insights.get('modelo_previsao', 'n/a')}",
        f"- Mes com pico medio de receita: {insights.get('pico_receita_mes', 'n/a')}",
        f"- Mes com pico medio de despesa: {insights.get('pico_despesa_mes', 'n/a')}",
        "",
        "## 5. Eficiencia dos Descontos",
        f"- Descontos totais no periodo: {formatar_moeda_brl(float(insights.get('descontos_total', 0.0)))}",
        (
            "- Correlacao entre descontos e receita do mes subsequente: "
            f"{float(insights.get('correlacao_descontos_receita_subsequente', 0.0)):.4f}"
        ),
        "",
        "## 6. Rentabilidade e Mix",
        "- Rentabilidade por centro de custo considera alocacao de despesas compartilhadas (RH, ADM e TI).",
        "- Mix de receita acompanha recorrencia (assinaturas) versus projetos pontuais.",
        "",
        "## 7. Produtividade, Anomalias e Benchmarking",
        (
            "- Despesa total de pessoal no periodo: "
            f"{formatar_moeda_brl(float(insights.get('despesa_pessoal_total', 0.0)))}"
        ),
        "- Deteccao de anomalias aplicada com z-score robusto por tipo de lancamento.",
        "- Benchmarking interno compara meses equivalentes entre anos consecutivos.",
        "",
        "## 8. Liquidez e Fluxo de Caixa",
        "- Datas de pagamento e recebimento foram geradas para estimar ciclo de caixa e capital de giro.",
        "",
        "## 9. Top 5 Categorias de Despesa",
        *linhas_despesas,
        "",
        "## 10. Resultado do Projeto",
        (
            "A solucao reduz dependencia de consolidacoes manuais, aumenta a confiabilidade "
            "dos dados e acelera a geracao de visoes executivas para acompanhamento financeiro."
        ),
        "",
        "## 11. Competencias Demonstradas",
        "- Automacao de rotinas operacionais com Python",
        "- Padronizacao de relatorios gerenciais",
        "- Tratamento e validacao de dados financeiros",
        "- Geracao de entregaveis executivos em multiplos formatos",
        "- Preparacao de publicacao web para portfolio profissional",
        "",
        "## 12. Entregaveis",
        "- 01_base_padronizada.csv",
        "- 02_resumo_mensal.csv",
        "- 03_resumo_categoria.csv",
        "- 04_indicadores_eficiencia.csv",
        "- 06_projecao_mensal.csv",
        "- 07_descontos_eficiencia.csv",
        "- 08_rentabilidade_centro_custo.csv",
        "- 09_mix_receita.csv",
        "- 10_produtividade_pessoal_projetos.csv",
        "- 11_anomalias.csv",
        "- 12_benchmarking_yoy.csv",
        "- 13_fluxo_caixa_liquidez.csv",
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
    analises: dict[str, Any],
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
        analises=analises,
    )
    salvar_payload_site(payload_site, pasta_site)
    classe_saldo = "saldo-ok" if kpis["saldo_total"] >= 0 else "saldo-alerta"
    payload_inline = json.dumps(payload_site, ensure_ascii=False).replace("</", "<\\/")

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
                    <h2>Resumo Executivo</h2>
                    <p class="summary-text">Painel reduzido para foco em indicadores-chave e decisoes gerenciais.</p>
                </div>
            </div>
            <div class="narrative">
                <section>
                    <h3>Objetivo</h3>
                    <p>Concentrar o que mais importa: receita, despesa, saldo, projecao, liquidez e rentabilidade por centro de custo.</p>
                </section>
                <section>
                    <h3>Leituras prioritarias</h3>
                    <ul>
                        <li>Projecao de receita e despesa para antecipar decisao comercial</li>
                        <li>Liquidez e ciclo de caixa para planejamento de capital de giro</li>
                        <li>Rentabilidade real por centro com alocacao de custos compartilhados</li>
                    </ul>
                </section>
                <section>
                    <h3>Analises complementares</h3>
                    <ul>
                        <li>Eficiencia dos descontos, mix de receita e produtividade</li>
                        <li>Deteccao de anomalias e benchmarking entre anos</li>
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
            <p class="status" id="dataset-status">Carregando indicadores principais...</p>
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
    <script id="embedded-data" type="application/json">{payload_inline}</script>
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
                    stageOrder: ["visao_geral", "mensal", "receitas", "projecao", "descontos", "rentabilidade", "liquidez"],
                    stages: {{
                        visao_geral: vazio,
                        mensal: vazio,
                        receitas: vazio,
                        projecao: vazio,
                        descontos: vazio,
                        rentabilidade: vazio,
                        liquidez: vazio,
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

            const stageOrder = ["visao_geral", "mensal", "receitas"];
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

            const analytics = sourceData.analytics || {{}};

            if (Array.isArray(analytics.sazonalidade) && analytics.sazonalidade.length) {{
                stageOrder.push("sazonalidade");
                const saz = analytics.sazonalidade;
                const picoReceita = saz.reduce((a, b) => Number(b.media_receita || 0) > Number(a.media_receita || 0) ? b : a, saz[0]);
                stages.sazonalidade = {{
                    label: "Sazonalidade",
                    title: "Picos sazonais e planejamento comercial",
                    description: "Identifica meses de pico para direcionar campanhas e alocacao de recursos.",
                    cards: [
                        {{ label: "Pico medio de receita", value: Number(picoReceita.media_receita || 0), format: "currency" }},
                        {{ label: "Mes de pico", value: picoReceita.mes_nome || "n/a", format: "text" }},
                        {{ label: "Media de saldo", value: saz.reduce((acc, item) => acc + Number(item.media_saldo || 0), 0) / saz.length, format: "currency" }},
                    ],
                    indices: [
                        {{ id: "media_receita", label: "Media de receita por mes", format: "currency", items: saz.map((item) => ({{ label: item.mes_nome, value: item.media_receita }})) }},
                        {{ id: "media_despesa", label: "Media de despesa por mes", format: "currency", items: saz.map((item) => ({{ label: item.mes_nome, value: item.media_despesa }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "mes_nome", label: "Mes", format: "text" }},
                            {{ key: "media_receita", label: "Media receita", format: "currency" }},
                            {{ key: "media_despesa", label: "Media despesa", format: "currency" }},
                            {{ key: "media_saldo", label: "Media saldo", format: "currency" }},
                        ],
                        rows: saz,
                    }},
                    highlight: "Use os meses de pico para reforcar capacidade comercial e operacao de entrega.",
                }};
            }}

            if (Array.isArray(analytics.projecao) && analytics.projecao.length) {{
                stageOrder.push("projecao");
                const proj = analytics.projecao;
                const futuros = proj.filter((item) => item.tipo === "projecao");
                const saldoPrev = futuros.reduce((acc, item) => acc + Number(item.saldo || 0), 0);
                stages.projecao = {{
                    label: "Projecao",
                    title: "Previsao de receitas e despesas",
                    description: "Serie temporal para apoiar previsao dos proximos meses.",
                    cards: [
                        {{ label: "Meses projetados", value: futuros.length, format: "int" }},
                        {{ label: "Saldo previsto", value: saldoPrev, format: "currency" }},
                        {{ label: "Modelo", value: sourceData.insights?.modelo_previsao || "n/a", format: "text" }},
                    ],
                    indices: [
                        {{ id: "receita_prev", label: "Receita (hist + proj)", format: "currency", items: proj.map((item) => ({{ label: item.ano_mes, value: item.total_receita }})) }},
                        {{ id: "despesa_prev", label: "Despesa (hist + proj)", format: "currency", items: proj.map((item) => ({{ label: item.ano_mes, value: item.total_despesa }})) }},
                        {{ id: "saldo_prev", label: "Saldo (hist + proj)", format: "currency", items: proj.map((item) => ({{ label: item.ano_mes, value: item.saldo }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "ano_mes", label: "Mes", format: "text" }},
                            {{ key: "tipo", label: "Serie", format: "text" }},
                            {{ key: "total_receita", label: "Receita", format: "currency" }},
                            {{ key: "total_despesa", label: "Despesa", format: "currency" }},
                            {{ key: "saldo", label: "Saldo", format: "currency" }},
                        ],
                        rows: proj,
                    }},
                    highlight: "Compare historico e projecao para antecipar ajustes de caixa e metas comerciais.",
                }};
            }}

            if (Array.isArray(analytics.descontos_mensal) && analytics.descontos_mensal.length) {{
                stageOrder.push("descontos");
                const desc = analytics.descontos_mensal;
                stages.descontos = {{
                    label: "Descontos",
                    title: "Eficiencia dos descontos comerciais",
                    description: "Correlacao com receita subsequente e payback medio por periodo.",
                    cards: [
                        {{ label: "Descontos totais", value: desc.reduce((acc, item) => acc + Number(item.descontos || 0), 0), format: "currency" }},
                        {{ label: "Correlacao", value: Number(sourceData.insights?.correlacao_descontos_receita_subsequente || 0) * 100, format: "percent" }},
                    ],
                    indices: [
                        {{ id: "desc_mes", label: "Descontos por mes", format: "currency", items: desc.map((item) => ({{ label: item.ano_mes, value: item.descontos }})) }},
                        {{ id: "payback_mes", label: "Payback mensal", format: "int", items: desc.map((item) => ({{ label: item.ano_mes, value: item.payback }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "ano_mes", label: "Mes", format: "text" }},
                            {{ key: "descontos", label: "Descontos", format: "currency" }},
                            {{ key: "receita_mes", label: "Receita", format: "currency" }},
                            {{ key: "receita_subsequente", label: "Receita seguinte", format: "currency" }},
                            {{ key: "payback", label: "Payback", format: "int" }},
                        ],
                        rows: desc,
                    }},
                    highlight: "Payback acima de 1 indica retorno maior do que o desconto no periodo seguinte.",
                }};
            }}

            if (Array.isArray(analytics.rentabilidade) && analytics.rentabilidade.length) {{
                stageOrder.push("rentabilidade");
                const rent = analytics.rentabilidade;
                stages.rentabilidade = {{
                    label: "Rentabilidade",
                    title: "Margem liquida por centro de custo",
                    description: "Inclui alocacao de despesas compartilhadas (RH, ADM e TI).",
                    cards: [
                        {{ label: "Centros analisados", value: rent.length, format: "int" }},
                        {{ label: "Maior margem liquida", value: Number(rent[0].margem_liquida || 0), format: "currency" }},
                    ],
                    indices: [
                        {{ id: "margem", label: "Margem liquida por centro", format: "currency", items: rent.map((item) => ({{ label: item.centro_custo, value: item.margem_liquida }})) }},
                        {{ id: "margem_pct", label: "Margem percentual", format: "percent", items: rent.map((item) => ({{ label: item.centro_custo, value: item.margem_percentual }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "centro_custo", label: "Centro", format: "text" }},
                            {{ key: "total_receita", label: "Receita", format: "currency" }},
                            {{ key: "despesa_total_alocada", label: "Despesa total", format: "currency" }},
                            {{ key: "margem_liquida", label: "Margem", format: "currency" }},
                            {{ key: "margem_percentual", label: "Margem %", format: "percent" }},
                        ],
                        rows: rent,
                    }},
                    highlight: "Centros de alta receita podem perder margem apos absorver custos de suporte.",
                }};
            }}

            if (Array.isArray(analytics.mix_receita) && analytics.mix_receita.length) {{
                stageOrder.push("mix_receita");
                const mix = analytics.mix_receita;
                stages.mix_receita = {{
                    label: "Mix de receita",
                    title: "Recorrencia versus projetos pontuais",
                    description: "Projetos aceleram crescimento e assinaturas estabilizam previsibilidade.",
                    cards: [
                        {{ label: "Share medio recorrente", value: mix.reduce((a, i) => a + Number(i.share_recorrente || 0), 0) / mix.length, format: "percent" }},
                        {{ label: "Share medio projetos", value: mix.reduce((a, i) => a + Number(i.share_projetos || 0), 0) / mix.length, format: "percent" }},
                    ],
                    indices: [
                        {{ id: "share_rec", label: "Share recorrente", format: "percent", items: mix.map((item) => ({{ label: item.ano_mes, value: item.share_recorrente }})) }},
                        {{ id: "share_proj", label: "Share projetos", format: "percent", items: mix.map((item) => ({{ label: item.ano_mes, value: item.share_projetos }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "ano_mes", label: "Mes", format: "text" }},
                            {{ key: "recorrentes_assinaturas", label: "Recorrente", format: "currency" }},
                            {{ key: "projetos_pontuais", label: "Projetos", format: "currency" }},
                            {{ key: "share_recorrente", label: "Share recorrente", format: "percent" }},
                            {{ key: "share_projetos", label: "Share projetos", format: "percent" }},
                        ],
                        rows: mix,
                    }},
                    highlight: "Equilibrar mix melhora previsibilidade sem limitar expansao por projetos.",
                }};
            }}

            if (Array.isArray(analytics.produtividade) && analytics.produtividade.length) {{
                stageOrder.push("produtividade");
                const prod = analytics.produtividade;
                stages.produtividade = {{
                    label: "Produtividade",
                    title: "Custo de pessoal por projeto",
                    description: "Relaciona despesa de pessoal com receita gerada em projetos.",
                    cards: [
                        {{ label: "Despesa pessoal total", value: prod.reduce((a, i) => a + Number(i.despesa_pessoal || 0), 0), format: "currency" }},
                        {{ label: "Receita projetos total", value: prod.reduce((a, i) => a + Number(i.receita_projetos || 0), 0), format: "currency" }},
                    ],
                    indices: [
                        {{ id: "ratio_prod", label: "Receita/Despesa pessoal", format: "int", items: prod.map((item) => ({{ label: item.ano_mes, value: item.produtividade_receita_por_pessoal }})) }},
                        {{ id: "custo_proj", label: "Custo pessoal por projeto", format: "currency", items: prod.map((item) => ({{ label: item.ano_mes, value: item.custo_pessoal_por_projeto }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "ano_mes", label: "Mes", format: "text" }},
                            {{ key: "receita_projetos", label: "Receita projetos", format: "currency" }},
                            {{ key: "despesa_pessoal", label: "Despesa pessoal", format: "currency" }},
                            {{ key: "qtd_projetos", label: "Qtde projetos", format: "int" }},
                            {{ key: "custo_pessoal_por_projeto", label: "Custo/projeto", format: "currency" }},
                        ],
                        rows: prod,
                    }},
                    highlight: "A produtividade sobe quando receita de projetos cresce mais que custo de pessoal.",
                }};
            }}

            if (Array.isArray(analytics.anomalias) && analytics.anomalias.length) {{
                stageOrder.push("anomalias");
                const ano = analytics.anomalias;
                stages.anomalias = {{
                    label: "Anomalias",
                    title: "Lancamentos atipicos",
                    description: "Valores fora do padrao podem indicar erro operacional ou oportunidade de revisao.",
                    cards: [
                        {{ label: "Qtde anomalias", value: ano.length, format: "int" }},
                        {{ label: "Maior valor atipico", value: Math.max(...ano.map((i) => Number(i.valor || 0))), format: "currency" }},
                    ],
                    indices: [
                        {{ id: "anomalia_valor", label: "Valor anomalo", format: "currency", items: ano.map((item) => ({{ label: item.ano_mes, value: item.valor }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "data", label: "Data", format: "text" }},
                            {{ key: "descricao", label: "Descricao", format: "text" }},
                            {{ key: "categoria", label: "Categoria", format: "text" }},
                            {{ key: "tipo", label: "Tipo", format: "text" }},
                            {{ key: "valor", label: "Valor", format: "currency" }},
                        ],
                        rows: ano,
                    }},
                    highlight: "Anomalias devem ser auditadas para evitar distorcoes no resultado gerencial.",
                }};
            }}

            if (Array.isArray(analytics.benchmarking) && analytics.benchmarking.length) {{
                stageOrder.push("benchmarking");
                const bench = analytics.benchmarking;
                stages.benchmarking = {{
                    label: "Benchmarking",
                    title: "Comparacao de meses equivalentes (YoY)",
                    description: "Mede crescimento organico entre anos para o mesmo mes calendario.",
                    cards: [
                        {{ label: "Meses comparaveis", value: bench.length, format: "int" }},
                        {{ label: "Crescimento medio receita", value: bench.reduce((a, i) => a + Number(i.crescimento_total_receita || 0), 0) / bench.length, format: "percent" }},
                    ],
                    indices: [
                        {{ id: "yoy_receita", label: "Crescimento receita YoY", format: "percent", items: bench.map((item) => ({{ label: item.ano_mes, value: item.crescimento_total_receita }})) }},
                        {{ id: "yoy_saldo", label: "Crescimento saldo YoY", format: "percent", items: bench.map((item) => ({{ label: item.ano_mes, value: item.crescimento_saldo }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "ano_mes", label: "Mes", format: "text" }},
                            {{ key: "total_receita_atual", label: "Receita atual", format: "currency" }},
                            {{ key: "total_receita_ano_anterior", label: "Receita ano anterior", format: "currency" }},
                            {{ key: "crescimento_total_receita", label: "Cresc. receita", format: "percent" }},
                            {{ key: "crescimento_saldo", label: "Cresc. saldo", format: "percent" }},
                        ],
                        rows: bench,
                    }},
                    highlight: "O benchmarking YoY reduz efeito sazonal e melhora leitura de crescimento real.",
                }};
            }}

            if (Array.isArray(analytics.liquidez) && analytics.liquidez.length) {{
                stageOrder.push("liquidez");
                const liq = analytics.liquidez;
                stages.liquidez = {{
                    label: "Liquidez",
                    title: "Fluxo de caixa e capital de giro",
                    description: "Leitura de entradas, saidas e ciclo de caixa com datas de pagamento/recebimento.",
                    cards: [
                        {{ label: "Saldo caixa acumulado", value: Number(liq[liq.length - 1].saldo_acumulado || 0), format: "currency" }},
                        {{ label: "Ciclo de caixa (dias)", value: Number(liq[0].ciclo_caixa_dias || 0), format: "int" }},
                        {{ label: "Pico capital de giro", value: Math.max(...liq.map((i) => Number(i.necessidade_capital_giro || 0))), format: "currency" }},
                    ],
                    indices: [
                        {{ id: "saldo_caixa", label: "Saldo de caixa mensal", format: "currency", items: liq.map((item) => ({{ label: item.ano_mes, value: item.saldo_caixa }})) }},
                        {{ id: "acumulado", label: "Saldo acumulado", format: "currency", items: liq.map((item) => ({{ label: item.ano_mes, value: item.saldo_acumulado }})) }},
                    ],
                    table: {{
                        columns: [
                            {{ key: "ano_mes", label: "Mes", format: "text" }},
                            {{ key: "entradas", label: "Entradas", format: "currency" }},
                            {{ key: "saidas", label: "Saidas", format: "currency" }},
                            {{ key: "saldo_caixa", label: "Saldo caixa", format: "currency" }},
                            {{ key: "saldo_acumulado", label: "Saldo acumulado", format: "currency" }},
                        ],
                        rows: liq,
                    }},
                    highlight: "A serie de caixa mostra janelas de pressao para antecipar capital de giro.",
                }};
            }}

            const etapasEssenciais = [
                "visao_geral",
                "mensal",
                "receitas",
                "projecao",
                "descontos",
                "rentabilidade",
                "liquidez",
            ];
            const stageOrderFiltrado = stageOrder.filter((id) => etapasEssenciais.includes(id));
            return {{ stageOrder: stageOrderFiltrado, stages }};
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
            const payloadEmbebido = document.getElementById("embedded-data");
            let carregouEmbebido = false;

            if (payloadEmbebido) {{
                try {{
                    sourceData = JSON.parse(payloadEmbebido.textContent || "{{}}");
                    rawRecords = Array.isArray(sourceData.records) ? sourceData.records : [];
                    popularFiltros();
                    atualizarDashboard();
                    carregouEmbebido = true;
                    datasetStatus.textContent = "Indicadores principais carregados. Atualizando base externa...";
                }} catch (erroEmbebido) {{
                    console.error(erroEmbebido);
                }}
            }}

            try {{
                const resposta = await fetch(DATA_URL, {{ cache: "no-store" }});
                if (!resposta.ok) {{
                    throw new Error(`Falha ao carregar ${{DATA_URL}}`);
                }}
                sourceData = await resposta.json();
                rawRecords = Array.isArray(sourceData.records) ? sourceData.records : [];
                popularFiltros();
                atualizarDashboard();
                datasetStatus.textContent = "Base externa atualizada.";
            }} catch (erro) {{
                if (carregouEmbebido) {{
                    datasetStatus.textContent = "Exibindo dados principais locais (base externa indisponivel).";
                    return;
                }}
                datasetStatus.textContent = "Nao foi possivel carregar os dados do dashboard. Execute o script novamente.";
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
    analises: dict[str, Any],
    pasta_saida: Path,
) -> bool:
    """Exporta DataFrames para CSV e, opcionalmente, para Excel."""
    pasta_saida.mkdir(parents=True, exist_ok=True)

    arquivos_csv = {
        "01_base_padronizada.csv": base,
        "02_resumo_mensal.csv": mensal,
        "03_resumo_categoria.csv": categoria,
        "04_indicadores_eficiencia.csv": indicadores,
        "06_projecao_mensal.csv": analises["projecao"],
        "07_descontos_eficiencia.csv": analises["descontos_mensal"],
        "07b_payback_descontos_categoria.csv": analises["payback_categoria"],
        "08_rentabilidade_centro_custo.csv": analises["rentabilidade"],
        "09_mix_receita.csv": analises["mix_receita"],
        "10_produtividade_pessoal_projetos.csv": analises["produtividade"],
        "11_anomalias.csv": analises["anomalias"],
        "12_benchmarking_yoy.csv": analises["benchmarking"],
        "13_fluxo_caixa_liquidez.csv": analises["liquidez"],
        "14_base_fluxo_com_datas.csv": analises["base_fluxo"],
        "15_sazonalidade_mensal.csv": analises["sazonalidade"],
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
            analises["projecao"].to_excel(writer, sheet_name="projecao", index=False)
            analises["rentabilidade"].to_excel(writer, sheet_name="rentabilidade", index=False)
            analises["liquidez"].to_excel(writer, sheet_name="liquidez", index=False)
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
    analises = gerar_analises_avancadas(base)
    base_fluxo = analises["base_fluxo"]

    excel_gerado = salvar_relatorios(
        base,
        resumo_mensal,
        resumo_categoria,
        indicadores,
        analises,
        pasta_saida,
    )

    arquivo_md = gerar_relatorio_executivo_markdown(
        resumo_mensal,
        resumo_categoria,
        indicadores,
        analises,
        pasta_saida,
        args.titulo_relatorio,
        args.nome_profissional,
        args.cargo_profissional,
        args.empresa,
        args.logo,
    )
    arquivo_html = gerar_relatorio_executivo_html(
        base_fluxo,
        resumo_mensal,
        resumo_categoria,
        indicadores,
        analises,
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
        pasta_saida / "06_projecao_mensal.csv",
        pasta_saida / "07_descontos_eficiencia.csv",
        pasta_saida / "07b_payback_descontos_categoria.csv",
        pasta_saida / "08_rentabilidade_centro_custo.csv",
        pasta_saida / "09_mix_receita.csv",
        pasta_saida / "10_produtividade_pessoal_projetos.csv",
        pasta_saida / "11_anomalias.csv",
        pasta_saida / "12_benchmarking_yoy.csv",
        pasta_saida / "13_fluxo_caixa_liquidez.csv",
        pasta_saida / "14_base_fluxo_com_datas.csv",
        pasta_saida / "15_sazonalidade_mensal.csv",
        arquivo_md,
        arquivo_html,
    ]:
        logger.info("  - %s", arq)

    if excel_gerado:
        logger.info("  - %s", pasta_saida / "relatorio_gerencial_padronizado.xlsx")


if __name__ == "__main__":
    main()