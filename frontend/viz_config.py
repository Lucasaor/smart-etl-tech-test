"""Configuração de visualizações por KPI — mapeamento tabela Gold → gráficos ideais.

Define os gráficos mais adequados para cada KPI baseado na descrição das análises.
Compatível com Streamlit (via st.plotly_chart) e Databricks (via displayHTML/display).
Usa exclusivamente Plotly para portabilidade.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable

import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from frontend.theme import (
    BRAND_COLORS,
    COLOR_CATEGORICAL,
    COLOR_OUTCOME,
    COLOR_SCALE,
    COLOR_SENTIMENT,
    COLOR_PERSONA,
    PLOTLY_LAYOUT,
)


# ─── Spec de visualização ────────────────────────────────────────────────────


@dataclass
class VizSpec:
    """Especificação de uma visualização para um KPI."""

    titulo: str
    tipo: str  # funnel, bar, pie, histogram, line, heatmap, gauge, scatter, table
    render_fn: Callable[[pl.DataFrame], go.Figure | None]
    descricao: str = ""


# ─── Paleta de cores (derivada do tema) ──────────────────────────────────────

CORES = {
    "primaria": BRAND_COLORS["primary"],
    "sucesso": BRAND_COLORS["highlight"],
    "perigo": BRAND_COLORS["dark"],
    "aviso": BRAND_COLORS["accent"],
    "neutro": "#64748B",
    "info": BRAND_COLORS["highlight"],
    "sequencial": COLOR_SCALE,
    "divergente": [BRAND_COLORS["dark"], BRAND_COLORS["primary"], BRAND_COLORS["highlight"]],
    "categorico": COLOR_CATEGORICAL,
}

OUTCOME_CORES = COLOR_OUTCOME

SENTIMENT_CORES = COLOR_SENTIMENT

PERSONA_CORES = COLOR_PERSONA


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _col_exists(df: pl.DataFrame, *nomes: str) -> str | None:
    """Retorna o primeiro nome de coluna que existe no DataFrame."""
    for n in nomes:
        if n in df.columns:
            return n
    # Busca parcial case-insensitive
    for n in nomes:
        for c in df.columns:
            if n.lower() in c.lower():
                return c
    return None


def _safe_to_pandas(df: pl.DataFrame) -> Any:
    """Converte para pandas tratando tipos problemáticos."""
    return df.to_pandas()


def _apply_layout(fig: go.Figure, title: str = "") -> go.Figure:
    """Aplica layout padrão do tema a um gráfico."""
    fig.update_layout(
        title=title,
        **PLOTLY_LAYOUT,
    )
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
# Renderizadores por KPI
# ═══════════════════════════════════════════════════════════════════════════════


# ─── Funil de Conversão ───────────────────────────────────────────────────────


def _funil_barra_outcomes(df: pl.DataFrame) -> go.Figure | None:
    col = _col_exists(df, "conversation_outcome", "outcome", "resultado")
    if not col:
        return None
    contagem = df.group_by(col).len().sort("len", descending=True)
    total = contagem["len"].sum()
    pdf = _safe_to_pandas(contagem)
    pdf["pct"] = (pdf["len"] / total * 100).round(1)
    pdf["label"] = pdf[col].str.replace("_", " ").str.title() + " (" + pdf["pct"].astype(str) + "%)"

    cores = [OUTCOME_CORES.get(v, "#aec7e8") for v in pdf[col]]
    fig = go.Figure(go.Bar(
        x=pdf["len"], y=pdf["label"],
        orientation="h", marker_color=cores,
        text=pdf["len"], textposition="auto",
    ))
    return _apply_layout(fig, "Distribuição por Outcome")


def _funil_taxa_conversao(df: pl.DataFrame) -> go.Figure | None:
    col_outcome = _col_exists(df, "conversation_outcome", "outcome")
    col_taxa = _col_exists(df, "taxa_conversao", "conversion_rate")

    if col_taxa and col_taxa in df.columns:
        taxa = df[col_taxa].mean()
    elif col_outcome:
        col_venda = _col_exists(df, "venda_fechada")
        if col_venda:
            taxa = df[col_venda].sum() / len(df) * 100 if len(df) > 0 else 0
        else:
            vendas = df.filter(pl.col(col_outcome) == "venda_fechada").height
            taxa = vendas / len(df) * 100 if len(df) > 0 else 0
    else:
        return None

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=float(taxa) if taxa is not None else 0,
        number={"suffix": "%"},
        title={"text": "Taxa de Conversão"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": BRAND_COLORS["accent"]},
            "steps": [
                {"range": [0, 30], "color": "#e8e0f4"},
                {"range": [30, 60], "color": "#b5d8d6"},
                {"range": [60, 100], "color": "#c5e8b0"},
            ],
        },
    ))
    return _apply_layout(fig)


def _funil_waterfall(df: pl.DataFrame) -> go.Figure | None:
    """Funil estilo waterfall mostrando perda em cada etapa."""
    col = _col_exists(df, "conversation_outcome", "outcome")
    col_count = _col_exists(df, "total_conversas", "contagem", "len", "count")
    if not col:
        return None

    # Definir ordem do funil
    ordem_funil = [
        "total", "proposta_enviada", "venda_fechada",
        "ghosting", "perdido_preco", "perdido_concorrente", "sem_interesse",
    ]

    if col_count and col_count in df.columns:
        contagem = {row[col]: row[col_count] for row in df.iter_rows(named=True)}
    else:
        contagem_df = df.group_by(col).len()
        contagem = {row[col]: row["len"] for row in contagem_df.iter_rows(named=True)}

    total = sum(contagem.values())
    labels = ["Total de Leads"]
    values = [total]
    measures = ["absolute"]

    for outcome in ordem_funil[1:]:
        if outcome in contagem:
            labels.append(outcome.replace("_", " ").title())
            if outcome == "venda_fechada":
                values.append(contagem[outcome])
                measures.append("absolute")
            else:
                values.append(-contagem[outcome])
                measures.append("relative")

    if len(labels) < 3:
        return None

    fig = go.Figure(go.Waterfall(
        x=labels, y=values, measure=measures,
        connector={"line": {"color": "#aaa"}},
        increasing={"marker": {"color": CORES["sucesso"]}},
        decreasing={"marker": {"color": CORES["perigo"]}},
        totals={"marker": {"color": CORES["primaria"]}},
    ))
    return _apply_layout(fig, "Funil de Conversão")


# ─── Lead Scoring ─────────────────────────────────────────────────────────────


def _lead_score_histogram(df: pl.DataFrame) -> go.Figure | None:
    col = _col_exists(df, "lead_score", "score", "score_total")
    if not col:
        return None
    fig = px.histogram(
        _safe_to_pandas(df), x=col, nbins=20,
        color_discrete_sequence=[CORES["primaria"]],
        labels={col: "Lead Score"},
    )
    fig.add_vline(
        x=float(df[col].mean()) if df[col].mean() is not None else 0,
        line_dash="dash", line_color=CORES["perigo"],
        annotation_text=f"Média: {df[col].mean():.1f}" if df[col].mean() is not None else "",
    )
    return _apply_layout(fig, "Distribuição de Lead Score")


def _lead_score_por_outcome(df: pl.DataFrame) -> go.Figure | None:
    col_score = _col_exists(df, "lead_score", "score", "score_total")
    col_outcome = _col_exists(df, "conversation_outcome", "outcome")
    if not col_score or not col_outcome:
        return None
    fig = px.box(
        _safe_to_pandas(df), x=col_outcome, y=col_score,
        color=col_outcome,
        color_discrete_map=OUTCOME_CORES,
        labels={col_score: "Lead Score", col_outcome: "Outcome"},
    )
    return _apply_layout(fig, "Lead Score por Outcome")


def _lead_score_componentes(df: pl.DataFrame) -> go.Figure | None:
    componentes = ["engajamento", "velocidade_resposta", "dados_compartilhados",
                    "profundidade", "veiculo"]
    cols_encontradas = {}
    for comp in componentes:
        col = _col_exists(df, f"score_{comp}", f"pts_{comp}", comp)
        if col:
            cols_encontradas[comp] = col
    if len(cols_encontradas) < 2:
        return None
    medias = {k.replace("_", " ").title(): float(df[v].mean() or 0) for k, v in cols_encontradas.items()}
    fig = go.Figure(go.Bar(
        x=list(medias.values()), y=list(medias.keys()),
        orientation="h", marker_color=CORES["categorico"][:len(medias)],
        text=[f"{v:.1f}" for v in medias.values()], textposition="auto",
    ))
    return _apply_layout(fig, "Componentes do Lead Score (Média)")


# ─── Performance por Campanha ─────────────────────────────────────────────────


def _campanha_conversao(df: pl.DataFrame) -> go.Figure | None:
    col_camp = _col_exists(df, "campaign_id", "campanha")
    col_taxa = _col_exists(df, "taxa_conversao", "conversion_rate", "taxa_conversao_pct")
    if not col_camp:
        return None

    if col_taxa:
        sorted_df = df.sort(col_taxa, descending=True)
    else:
        col_vendas = _col_exists(df, "vendas_fechadas", "vendas", "conversoes")
        col_total = _col_exists(df, "total_conversas", "total", "conversas")
        if col_vendas and col_total:
            sorted_df = df.with_columns(
                (pl.col(col_vendas) / pl.col(col_total) * 100).alias("_taxa")
            ).sort("_taxa", descending=True)
            col_taxa = "_taxa"
        else:
            return None

    pdf = _safe_to_pandas(sorted_df.head(15))
    fig = px.bar(
        pdf, x=col_camp, y=col_taxa,
        color=col_taxa,
        color_continuous_scale="RdYlGn",
        labels={col_camp: "Campanha", col_taxa: "Taxa de Conversão (%)"},
        text=col_taxa,
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    return _apply_layout(fig, "Taxa de Conversão por Campanha")


def _campanha_volume(df: pl.DataFrame) -> go.Figure | None:
    col_camp = _col_exists(df, "campaign_id", "campanha")
    col_total = _col_exists(df, "total_conversas", "total", "conversas")
    col_vendas = _col_exists(df, "vendas_fechadas", "vendas", "conversoes")
    if not col_camp or not col_total:
        return None

    pdf = _safe_to_pandas(df.sort(col_total, descending=True).head(15))

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=pdf[col_camp], y=pdf[col_total],
        name="Total Conversas", marker_color=CORES["primaria"],
    ))
    if col_vendas:
        fig.add_trace(go.Bar(
            x=pdf[col_camp], y=pdf[col_vendas],
            name="Vendas Fechadas", marker_color=CORES["sucesso"],
        ))
    fig.update_layout(barmode="overlay", bargap=0.3)
    return _apply_layout(fig, "Volume por Campanha")


# ─── Performance por Vendedor ─────────────────────────────────────────────────


def _vendedor_ranking(df: pl.DataFrame) -> go.Figure | None:
    col_agent = _col_exists(df, "agent_id", "vendedor", "seller_id")
    col_taxa = _col_exists(df, "taxa_conversao", "conversion_rate", "taxa_conversao_pct")
    if not col_agent:
        return None

    if not col_taxa:
        col_vendas = _col_exists(df, "vendas_fechadas", "vendas")
        col_total = _col_exists(df, "total_conversas", "total")
        if col_vendas and col_total:
            df = df.with_columns(
                (pl.col(col_vendas) / pl.col(col_total) * 100).alias("_taxa")
            )
            col_taxa = "_taxa"
        else:
            return None

    sorted_df = df.sort(col_taxa, descending=False)
    pdf = _safe_to_pandas(sorted_df.tail(15))

    cores = [CORES["sucesso"] if v >= 50 else CORES["aviso"] if v >= 25 else CORES["perigo"]
             for v in pdf[col_taxa]]
    fig = go.Figure(go.Bar(
        x=pdf[col_taxa], y=pdf[col_agent],
        orientation="h", marker_color=cores,
        text=[f"{v:.1f}%" for v in pdf[col_taxa]], textposition="auto",
    ))
    return _apply_layout(fig, "Ranking de Vendedores (Taxa de Conversão)")


def _vendedor_eficiencia(df: pl.DataFrame) -> go.Figure | None:
    col_agent = _col_exists(df, "agent_id", "vendedor")
    col_score = _col_exists(df, "score_eficiencia", "eficiencia", "efficiency_score")
    col_ghosting = _col_exists(df, "taxa_ghosting", "ghosting_rate", "taxa_ghosting_pct")
    if not col_agent or (not col_score and not col_ghosting):
        return None

    y_col = col_score or col_ghosting
    y_label = "Score de Eficiência" if col_score else "Taxa de Ghosting (%)"
    pdf = _safe_to_pandas(df.sort(y_col, descending=True).head(15))

    fig = px.scatter(
        pdf, x=col_agent, y=y_col,
        size=y_col, color=y_col,
        color_continuous_scale="RdYlGn" if col_score else "RdYlGn_r",
        labels={col_agent: "Vendedor", y_col: y_label},
    )
    return _apply_layout(fig, f"Vendedores — {y_label}")


# ─── Classificação de Personas ────────────────────────────────────────────────


def _personas_distribuicao(df: pl.DataFrame) -> go.Figure | None:
    col = _col_exists(df, "persona", "classificacao", "tipo_persona")
    if not col:
        return None
    contagem = df.group_by(col).len().sort("len", descending=True)
    pdf = _safe_to_pandas(contagem)
    cores = [PERSONA_CORES.get(v.lower(), "#aec7e8") for v in pdf[col]]

    fig = go.Figure(go.Pie(
        labels=pdf[col].str.replace("_", " ").str.title(),
        values=pdf["len"],
        marker=dict(colors=cores),
        hole=0.4,
        textinfo="label+percent",
        textposition="outside",
    ))
    return _apply_layout(fig, "Distribuição de Personas")


def _personas_confianca(df: pl.DataFrame) -> go.Figure | None:
    col_persona = _col_exists(df, "persona", "classificacao")
    col_conf = _col_exists(df, "score_confianca", "confianca", "confidence")
    if not col_persona or not col_conf:
        return None

    fig = px.box(
        _safe_to_pandas(df), x=col_persona, y=col_conf,
        color=col_persona,
        color_discrete_map={k: v for k, v in PERSONA_CORES.items()},
        labels={col_persona: "Persona", col_conf: "Score de Confiança"},
    )
    return _apply_layout(fig, "Confiança da Classificação por Persona")


# ─── Análise de Sentimento ────────────────────────────────────────────────────


def _sentimento_distribuicao(df: pl.DataFrame) -> go.Figure | None:
    col = _col_exists(df, "classificacao_sentimento", "sentimento", "sentiment",
                       "categoria_sentimento", "classificacao")
    if not col:
        col_score = _col_exists(df, "score_sentimento", "sentiment_score", "score")
        if not col_score:
            return None
        # Derivar classificação do score
        df = df.with_columns(
            pl.when(pl.col(col_score) > 0.2).then(pl.lit("positivo"))
            .when(pl.col(col_score) < -0.2).then(pl.lit("negativo"))
            .otherwise(pl.lit("neutro"))
            .alias("_sent_class")
        )
        col = "_sent_class"

    contagem = df.group_by(col).len().sort("len", descending=True)
    pdf = _safe_to_pandas(contagem)
    cores = [SENTIMENT_CORES.get(v.lower(), "#aec7e8") for v in pdf[col]]

    fig = go.Figure(go.Pie(
        labels=pdf[col].str.replace("_", " ").str.title(),
        values=pdf["len"],
        marker=dict(colors=cores),
        hole=0.4,
        textinfo="label+percent",
    ))
    return _apply_layout(fig, "Distribuição de Sentimento")


def _sentimento_score_hist(df: pl.DataFrame) -> go.Figure | None:
    col = _col_exists(df, "score_sentimento", "sentiment_score", "score")
    if not col:
        return None
    fig = px.histogram(
        _safe_to_pandas(df), x=col, nbins=30,
        color_discrete_sequence=[CORES["info"]],
        labels={col: "Score de Sentimento"},
    )
    fig.add_vline(x=0.2, line_dash="dash", line_color=CORES["sucesso"],
                  annotation_text="Limiar Positivo")
    fig.add_vline(x=-0.2, line_dash="dash", line_color=CORES["perigo"],
                  annotation_text="Limiar Negativo")
    return _apply_layout(fig, "Distribuição do Score de Sentimento")


# ─── Segmentação Multidimensional ─────────────────────────────────────────────


def _segmentacao_barras(df: pl.DataFrame) -> go.Figure | None:
    dimensoes = ["engajamento", "velocidade_resposta", "veiculo", "regiao",
                 "horario", "origem", "segmento_engajamento", "segmento_velocidade",
                 "segmento_veiculo", "segmento_regiao"]
    col = None
    for d in dimensoes:
        col = _col_exists(df, d, f"segmento_{d}")
        if col:
            break
    if not col:
        # Pegar a primeira coluna string com cardinalidade razoável
        for c in df.columns:
            if df[c].dtype == pl.Utf8 and 2 <= df[c].n_unique() <= 15:
                col = c
                break
    if not col:
        return None
    contagem = df.group_by(col).len().sort("len", descending=True)
    pdf = _safe_to_pandas(contagem)
    fig = px.bar(
        pdf, x=col, y="len", color=col,
        color_discrete_sequence=CORES["categorico"],
        labels={col: col.replace("_", " ").title(), "len": "Quantidade"},
    )
    return _apply_layout(fig, f"Segmentação por {col.replace('_', ' ').title()}")


def _segmentacao_heatmap(df: pl.DataFrame) -> go.Figure | None:
    """Heatmap cruzando duas dimensões de segmentação."""
    str_cols = [c for c in df.columns if df[c].dtype == pl.Utf8 and 2 <= df[c].n_unique() <= 15]
    if len(str_cols) < 2:
        return None

    col_x, col_y = str_cols[0], str_cols[1]
    cross = df.group_by([col_x, col_y]).len()
    pivot = cross.pivot(on=col_y, index=col_x, values="len").fill_null(0)

    y_labels = [str(v) for v in pivot[col_x].to_list()]
    z_cols = [c for c in pivot.columns if c != col_x]
    z_data = pivot.select(z_cols).to_numpy()

    fig = go.Figure(go.Heatmap(
        z=z_data, x=z_cols, y=y_labels,
        colorscale="Blues", texttemplate="%{z}",
    ))
    fig.update_layout(
        xaxis_title=col_y.replace("_", " ").title(),
        yaxis_title=col_x.replace("_", " ").title(),
    )
    return _apply_layout(fig, f"Cruzamento: {col_x.replace('_', ' ').title()} × {col_y.replace('_', ' ').title()}")


# ─── Análise Temporal ─────────────────────────────────────────────────────────


def _temporal_volume_diario(df: pl.DataFrame) -> go.Figure | None:
    col_data = _col_exists(df, "data", "dia", "date", "day")
    col_volume = _col_exists(df, "total_conversas", "volume", "conversas", "count", "len")
    if not col_data:
        return None
    if not col_volume:
        # Tentar agregar por data
        contagem = df.group_by(col_data).len().sort(col_data)
        col_volume = "len"
        pdf = _safe_to_pandas(contagem)
    else:
        pdf = _safe_to_pandas(df.sort(col_data))

    fig = px.area(
        pdf, x=col_data, y=col_volume,
        color_discrete_sequence=[CORES["primaria"]],
        labels={col_data: "Data", col_volume: "Volume"},
    )
    fig.update_traces(line=dict(width=2), fillcolor="rgba(31,119,180,0.2)")
    return _apply_layout(fig, "Volume Diário de Conversas")


def _temporal_dia_semana(df: pl.DataFrame) -> go.Figure | None:
    col_dia = _col_exists(df, "dia_semana", "weekday", "day_of_week")
    col_volume = _col_exists(df, "total_conversas", "vendas", "volume", "count", "len")
    if not col_dia:
        return None

    if col_volume:
        pdf = _safe_to_pandas(df)
    else:
        contagem = df.group_by(col_dia).len()
        col_volume = "len"
        pdf = _safe_to_pandas(contagem)

    # Ordenar dias da semana
    dias_ordem = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo",
                  "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
                  "seg", "ter", "qua", "qui", "sex", "sab", "dom",
                  "0", "1", "2", "3", "4", "5", "6"]

    fig = px.bar(
        pdf, x=col_dia, y=col_volume,
        color_discrete_sequence=[CORES["info"]],
        labels={col_dia: "Dia da Semana", col_volume: "Volume"},
    )
    return _apply_layout(fig, "Distribuição por Dia da Semana")


def _temporal_faixa_horaria(df: pl.DataFrame) -> go.Figure | None:
    col_hora = _col_exists(df, "hora", "faixa_horaria", "hour")
    col_volume = _col_exists(df, "total_conversas", "vendas", "volume", "count", "len")
    if not col_hora:
        return None

    if col_volume:
        pdf = _safe_to_pandas(df.sort(col_hora))
    else:
        contagem = df.group_by(col_hora).len().sort(col_hora)
        col_volume = "len"
        pdf = _safe_to_pandas(contagem)

    fig = px.bar(
        pdf, x=col_hora, y=col_volume,
        color_discrete_sequence=[CORES["aviso"]],
        labels={col_hora: "Hora", col_volume: "Volume"},
    )
    return _apply_layout(fig, "Picos de Demanda por Faixa Horária")


# ─── Análise de Concorrência ─────────────────────────────────────────────────


def _concorrencia_mencoes(df: pl.DataFrame) -> go.Figure | None:
    col_conc = _col_exists(df, "concorrente", "competitor", "seguradora")
    col_freq = _col_exists(df, "frequencia", "mencoes", "total", "count", "len")
    if not col_conc:
        return None
    if not col_freq:
        contagem = df.group_by(col_conc).len().sort("len", descending=True)
        col_freq = "len"
        pdf = _safe_to_pandas(contagem)
    else:
        pdf = _safe_to_pandas(df.sort(col_freq, descending=True))

    fig = px.bar(
        pdf, x=col_freq, y=col_conc, orientation="h",
        color=col_freq, color_continuous_scale="Reds",
        labels={col_conc: "Concorrente", col_freq: "Menções"},
        text=col_freq,
    )
    fig.update_traces(textposition="auto")
    return _apply_layout(fig, "Menções por Concorrente")


def _concorrencia_taxa_perda(df: pl.DataFrame) -> go.Figure | None:
    col_conc = _col_exists(df, "concorrente", "competitor")
    col_taxa = _col_exists(df, "taxa_perda", "loss_rate", "taxa_perda_pct")
    if not col_conc or not col_taxa:
        return None
    pdf = _safe_to_pandas(df.sort(col_taxa, descending=True))
    fig = px.bar(
        pdf, x=col_conc, y=col_taxa,
        color=col_taxa, color_continuous_scale="YlOrRd",
        labels={col_conc: "Concorrente", col_taxa: "Taxa de Perda (%)"},
        text=col_taxa,
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    return _apply_layout(fig, "Taxa de Perda por Concorrente")


def _concorrencia_impacto(df: pl.DataFrame) -> go.Figure | None:
    """Compara taxa de perda com/sem menção a concorrente."""
    col_mencao = _col_exists(df, "menciona_concorrente", "com_concorrente")
    col_taxa = _col_exists(df, "taxa_perda", "taxa_conversao", "conversion_rate")
    if not col_mencao or not col_taxa:
        return None
    pdf = _safe_to_pandas(df)
    fig = px.bar(
        pdf, x=col_mencao, y=col_taxa,
        color=col_mencao,
        color_discrete_sequence=[CORES["sucesso"], CORES["perigo"]],
        labels={col_mencao: "Menciona Concorrente", col_taxa: "Taxa (%)"},
    )
    return _apply_layout(fig, "Impacto da Menção a Concorrentes")


# ═══════════════════════════════════════════════════════════════════════════════
# Mapeamento tabela → visualizações
# ═══════════════════════════════════════════════════════════════════════════════


# Padrões regex para identificar KPIs pelo nome da tabela
_KPI_VIZSPECS: list[tuple[str, list[VizSpec]]] = [
    (
        r"funil.*(convers|conversion)",
        [
            VizSpec("Funil de Conversão", "waterfall", _funil_waterfall,
                    "Visualização do funil mostrando perda em cada etapa"),
            VizSpec("Distribuição por Outcome", "bar", _funil_barra_outcomes,
                    "Distribuição horizontal dos outcomes de conversa"),
            VizSpec("Taxa de Conversão", "gauge", _funil_taxa_conversao,
                    "Indicador gauge da taxa de conversão geral"),
        ],
    ),
    (
        r"lead.?scor",
        [
            VizSpec("Distribuição de Lead Score", "histogram", _lead_score_histogram,
                    "Histograma mostrando a distribuição dos scores"),
            VizSpec("Lead Score por Outcome", "box", _lead_score_por_outcome,
                    "Box plot comparando scores por resultado da conversa"),
            VizSpec("Componentes do Score", "bar", _lead_score_componentes,
                    "Contribuição média de cada componente do score"),
        ],
    ),
    (
        r"performance.*(campanha|campaign)",
        [
            VizSpec("Taxa de Conversão por Campanha", "bar", _campanha_conversao,
                    "Ranking de campanhas por taxa de conversão"),
            VizSpec("Volume por Campanha", "bar", _campanha_volume,
                    "Total de conversas e vendas por campanha"),
        ],
    ),
    (
        r"performance.*(vendedor|seller|agent)",
        [
            VizSpec("Ranking de Vendedores", "bar", _vendedor_ranking,
                    "Ranking horizontal de vendedores por taxa de conversão"),
            VizSpec("Eficiência dos Vendedores", "scatter", _vendedor_eficiencia,
                    "Score de eficiência ou taxa de ghosting por vendedor"),
        ],
    ),
    (
        r"(classifica|persona)",
        [
            VizSpec("Distribuição de Personas", "pie", _personas_distribuicao,
                    "Proporção de cada tipo de persona (donut chart)"),
            VizSpec("Confiança por Persona", "box", _personas_confianca,
                    "Distribuição do score de confiança por persona"),
        ],
    ),
    (
        r"sentimento|sentiment",
        [
            VizSpec("Distribuição de Sentimento", "pie", _sentimento_distribuicao,
                    "Proporção positivo/neutro/negativo"),
            VizSpec("Histograma de Sentimento", "histogram", _sentimento_score_hist,
                    "Distribuição contínua do score de sentimento"),
        ],
    ),
    (
        r"segmenta",
        [
            VizSpec("Segmentação Principal", "bar", _segmentacao_barras,
                    "Distribuição da principal dimensão de segmentação"),
            VizSpec("Cruzamento de Segmentos", "heatmap", _segmentacao_heatmap,
                    "Heatmap cruzando duas dimensões de segmentação"),
        ],
    ),
    (
        r"temporal|time|tempo",
        [
            VizSpec("Volume Diário", "area", _temporal_volume_diario,
                    "Evolução do volume de conversas ao longo do tempo"),
            VizSpec("Dia da Semana", "bar", _temporal_dia_semana,
                    "Distribuição de volume por dia da semana"),
            VizSpec("Picos por Hora", "bar", _temporal_faixa_horaria,
                    "Identificação de picos de demanda por faixa horária"),
        ],
    ),
    (
        r"concorr|competitor|competit",
        [
            VizSpec("Menções por Concorrente", "bar", _concorrencia_mencoes,
                    "Frequência de menção a cada concorrente"),
            VizSpec("Taxa de Perda por Concorrente", "bar", _concorrencia_taxa_perda,
                    "Taxa de perda quando cada concorrente é mencionado"),
            VizSpec("Impacto da Concorrência", "bar", _concorrencia_impacto,
                    "Comparação de resultados com/sem menção a concorrente"),
        ],
    ),
]


def get_viz_specs(table_name: str) -> list[VizSpec]:
    """Retorna as especificações de visualização para uma tabela Gold.

    Identifica o KPI pelo nome da tabela usando padrões regex.
    Retorna lista vazia se o KPI não for reconhecido (fallback genérico).
    """
    norm = table_name.lower().replace("-", "_")
    for pattern, specs in _KPI_VIZSPECS:
        if re.search(pattern, norm):
            return specs
    return []


def render_kpi_charts(
    table_name: str,
    df: pl.DataFrame,
) -> list[tuple[str, go.Figure]]:
    """Renderiza os gráficos para um KPI, retornando lista de (titulo, fig).

    Tenta cada VizSpec registrada para o KPI. Ignora specs cujo render_fn
    retorna None (colunas ausentes no DataFrame).
    """
    specs = get_viz_specs(table_name)
    results = []
    for spec in specs:
        try:
            fig = spec.render_fn(df)
            if fig is not None:
                results.append((spec.titulo, fig))
        except Exception:
            pass
    return results
