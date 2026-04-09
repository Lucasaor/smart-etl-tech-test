"""Tests for Phase 4 — Gold Layer (sentiment, personas, segmentation, analytics, vendor)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from core.storage import LocalDeltaBackend


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _make_silver_conversations(**kwargs) -> pl.DataFrame:
    """Cria um DataFrame de Silver conversations para testes Gold."""
    defaults = {
        "conversation_id": [
            "conv_001", "conv_002", "conv_003", "conv_004",
            "conv_005", "conv_006", "conv_007",
        ],
        "first_message_at": pl.Series([
            "2026-02-01 09:00:00",
            "2026-02-01 10:00:00",
            "2026-02-02 08:30:00",
            "2026-02-02 14:00:00",
            "2026-02-03 09:15:00",
            "2026-02-03 11:00:00",
            "2026-02-04 16:00:00",
        ], dtype=pl.Datetime),
        "last_message_at": pl.Series([
            "2026-02-01 09:30:00",
            "2026-02-01 10:05:00",
            "2026-02-02 10:00:00",
            "2026-02-02 14:02:00",
            "2026-02-03 11:45:00",
            "2026-02-03 15:30:00",
            "2026-02-04 16:10:00",
        ], dtype=pl.Datetime),
        "total_messages": [18, 3, 12, 2, 25, 8, 5],
        "inbound_messages": [9, 1, 6, 0, 14, 4, 2],
        "outbound_messages": [9, 2, 6, 2, 11, 4, 3],
        "audio_messages": [2, 0, 1, 0, 3, 0, 1],
        "image_messages": [1, 0, 0, 0, 2, 0, 0],
        "audio_transcriptions": [2, 0, 1, 0, 3, 0, 1],
        "has_cpf": [True, False, True, False, True, False, False],
        "has_email": [True, False, False, False, True, True, False],
        "has_cep": [True, False, False, False, False, False, False],
        "has_phone": [False, False, True, False, True, False, False],
        "lead_plate": ["SYL8V26", None, "ABC1D23", None, "XYZ9W12", None, None],
        "lead_cep": ["08617986", None, None, None, None, None, None],
        "competitor_mentioned": [
            "porto seguro", None, None, None, "hdi seguros", "azul seguros", None,
        ],
        "conversation_outcome": [
            "venda_fechada",
            "ghosting",
            "venda_fechada",
            "ghosting",
            "perdido_concorrente",
            "proposta_enviada",
            "em_negociacao",
        ],
        "agent_id": [
            "agent_marcos_07", "agent_marcos_07", "agent_ana_03",
            "agent_ana_03", "agent_marcos_07", "agent_ana_03", "agent_ana_03",
        ],
        "campaign_id": [
            "camp_landing_fev2026", "camp_landing_fev2026",
            "camp_google_fev2026", "camp_google_fev2026",
            "camp_landing_fev2026", "camp_google_fev2026",
            "camp_landing_fev2026",
        ],
        "channel": ["whatsapp"] * 7,
        "lead_source": [
            "google_ads", "facebook", "google_ads", "facebook",
            "google_ads", "indicacao", "instagram",
        ],
        "lead_city": [
            "São Paulo", "Rio de Janeiro", "Belo Horizonte", "São Paulo",
            "Curitiba", "São Paulo", "Salvador",
        ],
        "lead_state": ["SP", "RJ", "MG", "SP", "PR", "SP", "BA"],
        "lead_device": ["android", "iphone", "desktop", "android", "iphone", "desktop", "android"],
        "avg_response_time_sec": [90.0, 500.0, 150.0, None, 45.0, 200.0, 300.0],
        "lead_phone": [
            "+5511988880001", "+5521977770001", "+5531966660001",
            "+5511955550001", "+5541944440001", "+5511933330001",
            "+5571922220001",
        ],
        "lead_name": [
            "Ana Paula", "João Silva", "Maria Santos",
            "Pedro Lima", "Carlos Mendes", "Fernanda Costa",
            "Lucas Almeida",
        ],
        "duration_minutes": [30.0, 5.0, 90.0, 2.0, 150.0, 270.0, 10.0],
        "conversation_date": pl.Series([
            "2026-02-01", "2026-02-01", "2026-02-02", "2026-02-02",
            "2026-02-03", "2026-02-03", "2026-02-04",
        ], dtype=pl.Date),
    }
    defaults.update(kwargs)
    return pl.DataFrame(defaults)


def _write_silver_conversations(tmp_path: Path) -> str:
    """Escreve tabela Silver conversations em Delta para testes."""
    conv_path = str(tmp_path / "silver_conversations")
    backend = LocalDeltaBackend()
    df = _make_silver_conversations()
    backend.write_table(df, conv_path, mode="overwrite", schema_mode="overwrite")
    return conv_path


# ─── Settings tests ───────────────────────────────────────────────────────────


class TestSettingsGoldPaths:
    """Testa os paths Gold adicionados ao Settings."""

    def test_gold_sentiment_path_local(self, monkeypatch):
        from config.settings import Settings

        monkeypatch.delenv("RUNTIME_ENV", raising=False)
        s = Settings(data_root="/tmp/data")
        assert s.gold_sentiment_path == "/tmp/data/gold/sentiment"

    def test_gold_personas_path_local(self, monkeypatch):
        from config.settings import Settings

        monkeypatch.delenv("RUNTIME_ENV", raising=False)
        s = Settings(data_root="/tmp/data")
        assert s.gold_personas_path == "/tmp/data/gold/personas"

    def test_gold_segmentation_path_local(self, monkeypatch):
        from config.settings import Settings

        monkeypatch.delenv("RUNTIME_ENV", raising=False)
        s = Settings(data_root="/tmp/data")
        assert s.gold_segmentation_path == "/tmp/data/gold/segmentation"

    def test_gold_analytics_path_local(self, monkeypatch):
        from config.settings import Settings

        monkeypatch.delenv("RUNTIME_ENV", raising=False)
        s = Settings(data_root="/tmp/data")
        assert s.gold_analytics_path == "/tmp/data/gold/analytics"

    def test_gold_vendor_path_local(self, monkeypatch):
        from config.settings import Settings

        monkeypatch.delenv("RUNTIME_ENV", raising=False)
        s = Settings(data_root="/tmp/data")
        assert s.gold_vendor_path == "/tmp/data/gold/vendor_analysis"

    def test_gold_paths_databricks(self, monkeypatch):
        from config.settings import Settings

        monkeypatch.delenv("RUNTIME_ENV", raising=False)
        s = Settings(runtime_env="databricks")
        assert s.gold_sentiment_path == "/mnt/delta/gold/sentiment"
        assert s.gold_vendor_path == "/mnt/delta/gold/vendor_analysis"


# ─── Sentiment tests ──────────────────────────────────────────────────────────


class TestCalcularSentimentoHeuristico:
    def test_venda_fechada_positivo(self):
        from pipeline.gold.sentiment import calcular_sentimento_heuristico

        df = _make_silver_conversations()
        result = calcular_sentimento_heuristico(df)

        # conv_001: venda_fechada com bom engajamento → positivo
        row = result.filter(pl.col("conversation_id") == "conv_001")
        assert row["sentimento"][0] == "positivo"
        assert row["sentimento_score"][0] > 0.2

    def test_ghosting_negativo(self):
        from pipeline.gold.sentiment import calcular_sentimento_heuristico

        df = _make_silver_conversations()
        result = calcular_sentimento_heuristico(df)

        # conv_002: ghosting + poucas msgs → negativo
        row = result.filter(pl.col("conversation_id") == "conv_002")
        assert row["sentimento"][0] == "negativo"
        assert row["sentimento_score"][0] < -0.2

    def test_columns_added(self):
        from pipeline.gold.sentiment import calcular_sentimento_heuristico

        df = _make_silver_conversations()
        result = calcular_sentimento_heuristico(df)
        assert "sentimento" in result.columns
        assert "sentimento_score" in result.columns
        assert "sentimento_fatores" in result.columns

    def test_all_rows_classified(self):
        from pipeline.gold.sentiment import calcular_sentimento_heuristico

        df = _make_silver_conversations()
        result = calcular_sentimento_heuristico(df)
        sentimentos = result["sentimento"].to_list()
        assert all(s in ("positivo", "neutro", "negativo") for s in sentimentos)
        assert len(result) == len(df)


class TestGerarGoldSentiment:
    def test_generates_table(self, tmp_path):
        from pipeline.gold.sentiment import gerar_gold_sentiment

        conv_path = _write_silver_conversations(tmp_path)
        gold_path = str(tmp_path / "gold_sentiment")

        stats = gerar_gold_sentiment(conv_path, gold_path)
        assert stats["total_conversas"] == 7
        assert stats["rows_written"] == 7
        assert stats["positivo"] + stats["neutro"] + stats["negativo"] == 7

    def test_output_columns(self, tmp_path):
        from pipeline.gold.sentiment import gerar_gold_sentiment

        conv_path = _write_silver_conversations(tmp_path)
        gold_path = str(tmp_path / "gold_sentiment")
        gerar_gold_sentiment(conv_path, gold_path)

        backend = LocalDeltaBackend()
        result = backend.read_table(gold_path)
        for col in ["conversation_id", "sentimento", "sentimento_score", "sentimento_fatores"]:
            assert col in result.columns, f"Missing column: {col}"


# ─── Personas tests ───────────────────────────────────────────────────────────


class TestClassificarPersonas:
    def test_ghosting_becomes_fantasma(self):
        from pipeline.gold.personas import classificar_personas

        df = _make_silver_conversations()
        result = classificar_personas(df)

        # conv_002: ghosting + 1 inbound msg → Fantasma
        row = result.filter(pl.col("conversation_id") == "conv_002")
        assert row["persona"][0] == "Fantasma"

    def test_venda_curta_becomes_decidido(self):
        from pipeline.gold.personas import classificar_personas

        df = _make_silver_conversations()
        result = classificar_personas(df)

        # conv_003: venda_fechada + 12 msgs → Decidido
        row = result.filter(pl.col("conversation_id") == "conv_003")
        assert row["persona"][0] == "Decidido"

    def test_long_conversation_with_competitor_becomes_negociador(self):
        from pipeline.gold.personas import classificar_personas

        df = _make_silver_conversations()
        result = classificar_personas(df)

        # conv_005: 25 msgs + concorrente + perdido_concorrente → Negociador
        row = result.filter(pl.col("conversation_id") == "conv_005")
        assert row["persona"][0] == "Negociador"

    def test_all_have_persona(self):
        from pipeline.gold.personas import classificar_personas

        df = _make_silver_conversations()
        result = classificar_personas(df)
        personas = result["persona"].to_list()
        valid = {"Fantasma", "Decidido", "Negociador", "Pesquisador", "Indeciso"}
        assert all(p in valid for p in personas)
        assert len(result) == len(df)

    def test_confianca_between_0_and_1(self):
        from pipeline.gold.personas import classificar_personas

        df = _make_silver_conversations()
        result = classificar_personas(df)
        confiancas = result["persona_confianca"].to_list()
        assert all(0 <= c <= 1.0 for c in confiancas)


class TestGerarGoldPersonas:
    def test_generates_table(self, tmp_path):
        from pipeline.gold.personas import gerar_gold_personas

        conv_path = _write_silver_conversations(tmp_path)
        gold_path = str(tmp_path / "gold_personas")

        stats = gerar_gold_personas(conv_path, gold_path)
        assert stats["total_conversas"] == 7
        assert stats["rows_written"] == 7

    def test_output_columns(self, tmp_path):
        from pipeline.gold.personas import gerar_gold_personas

        conv_path = _write_silver_conversations(tmp_path)
        gold_path = str(tmp_path / "gold_personas")
        gerar_gold_personas(conv_path, gold_path)

        backend = LocalDeltaBackend()
        result = backend.read_table(gold_path)
        for col in ["conversation_id", "persona", "persona_confianca", "persona_fatores"]:
            assert col in result.columns, f"Missing column: {col}"


# ─── Segmentation tests ──────────────────────────────────────────────────────


class TestSegmentarConversas:
    def test_engamento_segments(self):
        from pipeline.gold.segmentation import segmentar_conversas

        df = _make_silver_conversations()
        result = segmentar_conversas(df)

        # conv_005: 25 msgs → alto
        row = result.filter(pl.col("conversation_id") == "conv_005")
        assert row["seg_engajamento"][0] == "alto"

        # conv_002: 3 msgs → baixo
        row = result.filter(pl.col("conversation_id") == "conv_002")
        assert row["seg_engajamento"][0] == "baixo"

    def test_regiao_segments(self):
        from pipeline.gold.segmentation import segmentar_conversas

        df = _make_silver_conversations()
        result = segmentar_conversas(df)

        # SP → sudeste
        row = result.filter(pl.col("conversation_id") == "conv_001")
        assert row["seg_regiao"][0] == "sudeste"

        # PR → sul
        row = result.filter(pl.col("conversation_id") == "conv_005")
        assert row["seg_regiao"][0] == "sul"

        # BA → nordeste
        row = result.filter(pl.col("conversation_id") == "conv_007")
        assert row["seg_regiao"][0] == "nordeste"

    def test_veiculo_segment(self):
        from pipeline.gold.segmentation import segmentar_conversas

        df = _make_silver_conversations()
        result = segmentar_conversas(df)

        # conv_001: has plate → com_veiculo
        row = result.filter(pl.col("conversation_id") == "conv_001")
        assert row["seg_veiculo"][0] == "com_veiculo"

        # conv_002: no plate → sem_veiculo
        row = result.filter(pl.col("conversation_id") == "conv_002")
        assert row["seg_veiculo"][0] == "sem_veiculo"

    def test_qualificacao_lead(self):
        from pipeline.gold.segmentation import segmentar_conversas

        df = _make_silver_conversations()
        result = segmentar_conversas(df)

        # conv_001: has_cpf=True + has_email=True → alta_qualificacao
        row = result.filter(pl.col("conversation_id") == "conv_001")
        assert row["seg_qualificacao_lead"][0] == "alta_qualificacao"

    def test_all_segment_columns_present(self):
        from pipeline.gold.segmentation import segmentar_conversas

        df = _make_silver_conversations()
        result = segmentar_conversas(df)
        expected_cols = [
            "seg_engajamento", "seg_velocidade_resposta", "seg_veiculo",
            "seg_regiao", "seg_origem", "seg_duracao", "seg_qualificacao_lead",
        ]
        for col in expected_cols:
            assert col in result.columns, f"Missing segment column: {col}"


class TestGerarGoldSegmentation:
    def test_generates_table(self, tmp_path):
        from pipeline.gold.segmentation import gerar_gold_segmentation

        conv_path = _write_silver_conversations(tmp_path)
        gold_path = str(tmp_path / "gold_segmentation")

        stats = gerar_gold_segmentation(conv_path, gold_path)
        assert stats["total_conversas"] == 7
        assert stats["rows_written"] == 7
        assert "seg_engajamento" in stats
        assert "seg_regiao" in stats


# ─── Analytics tests ──────────────────────────────────────────────────────────


class TestCalcularFunilConversao:
    def test_funil_counts(self):
        from pipeline.gold.analytics import calcular_funil_conversao

        df = _make_silver_conversations()
        funil = calcular_funil_conversao(df)

        assert "conversation_outcome" in funil.columns
        assert "contagem" in funil.columns
        assert "percentual" in funil.columns
        assert funil["contagem"].sum() == 7

    def test_funil_percentual_sums_to_100(self):
        from pipeline.gold.analytics import calcular_funil_conversao

        df = _make_silver_conversations()
        funil = calcular_funil_conversao(df)
        total_pct = funil["percentual"].sum()
        assert abs(total_pct - 100.0) < 0.1

    def test_empty_dataframe(self):
        from pipeline.gold.analytics import calcular_funil_conversao

        df = pl.DataFrame(schema={"conversation_outcome": pl.Utf8})
        funil = calcular_funil_conversao(df)
        assert len(funil) == 0


class TestCalcularLeadScore:
    def test_score_range(self):
        from pipeline.gold.analytics import calcular_lead_score

        df = _make_silver_conversations()
        result = calcular_lead_score(df)
        scores = result["lead_score"].to_list()
        assert all(0 <= s <= 100 for s in scores)

    def test_high_engagement_higher_score(self):
        from pipeline.gold.analytics import calcular_lead_score

        df = _make_silver_conversations()
        result = calcular_lead_score(df)

        # conv_005: 25 msgs, fast response, lots of PII → high score
        score_005 = result.filter(
            pl.col("conversation_id") == "conv_005"
        )["lead_score"][0]

        # conv_004: 2 msgs, no inbound, no PII → low score
        score_004 = result.filter(
            pl.col("conversation_id") == "conv_004"
        )["lead_score"][0]

        assert score_005 > score_004


class TestCalcularPerformanceCampanha:
    def test_campaign_metrics(self):
        from pipeline.gold.analytics import calcular_performance_campanha

        df = _make_silver_conversations()
        result = calcular_performance_campanha(df)

        assert "campaign_id" in result.columns
        assert "total_conversas" in result.columns
        assert "vendas" in result.columns
        assert "taxa_conversao_pct" in result.columns
        assert len(result) == 2  # 2 campaigns in fixture


class TestGerarGoldAnalytics:
    def test_generates_table(self, tmp_path):
        from pipeline.gold.analytics import gerar_gold_analytics

        conv_path = _write_silver_conversations(tmp_path)
        gold_path = str(tmp_path / "gold_analytics")

        stats = gerar_gold_analytics(conv_path, gold_path)
        assert stats["total_conversas"] == 7
        assert stats["rows_written"] == 7
        assert "lead_score_medio" in stats
        assert "funil" in stats
        assert "campanhas" in stats

    def test_output_has_lead_score(self, tmp_path):
        from pipeline.gold.analytics import gerar_gold_analytics

        conv_path = _write_silver_conversations(tmp_path)
        gold_path = str(tmp_path / "gold_analytics")
        gerar_gold_analytics(conv_path, gold_path)

        backend = LocalDeltaBackend()
        result = backend.read_table(gold_path)
        assert "lead_score" in result.columns
        assert all(0 <= s <= 100 for s in result["lead_score"].to_list())


# ─── Vendor Analysis tests ───────────────────────────────────────────────────


class TestCalcularMetricasVendedor:
    def test_vendor_count(self):
        from pipeline.gold.vendor_analysis import calcular_metricas_vendedor

        df = _make_silver_conversations()
        result = calcular_metricas_vendedor(df)

        # 2 vendedores: agent_marcos_07 e agent_ana_03
        assert len(result) == 2

    def test_conversion_rates(self):
        from pipeline.gold.vendor_analysis import calcular_metricas_vendedor

        df = _make_silver_conversations()
        result = calcular_metricas_vendedor(df)

        for row in result.iter_rows(named=True):
            assert 0 <= row["taxa_conversao_pct"] <= 100
            assert 0 <= row["taxa_ghosting_pct"] <= 100

    def test_vendor_score_present(self):
        from pipeline.gold.vendor_analysis import calcular_metricas_vendedor

        df = _make_silver_conversations()
        result = calcular_metricas_vendedor(df)
        assert "score_vendedor" in result.columns

    def test_outcome_columns_present(self):
        from pipeline.gold.vendor_analysis import calcular_metricas_vendedor

        df = _make_silver_conversations()
        result = calcular_metricas_vendedor(df)
        for col in [
            "vendas_fechadas", "ghosting", "perdido_preco",
            "perdido_concorrente", "propostas_enviadas",
        ]:
            assert col in result.columns, f"Missing: {col}"


class TestGerarGoldVendor:
    def test_generates_table(self, tmp_path):
        from pipeline.gold.vendor_analysis import gerar_gold_vendor

        conv_path = _write_silver_conversations(tmp_path)
        gold_path = str(tmp_path / "gold_vendor")

        stats = gerar_gold_vendor(conv_path, gold_path)
        assert stats["total_vendedores"] == 2
        assert stats["rows_written"] == 2

    def test_output_columns(self, tmp_path):
        from pipeline.gold.vendor_analysis import gerar_gold_vendor

        conv_path = _write_silver_conversations(tmp_path)
        gold_path = str(tmp_path / "gold_vendor")
        gerar_gold_vendor(conv_path, gold_path)

        backend = LocalDeltaBackend()
        result = backend.read_table(gold_path)
        for col in ["agent_id", "total_conversas", "taxa_conversao_pct", "score_vendedor"]:
            assert col in result.columns, f"Missing: {col}"


# ─── Orchestrator Gold step test ─────────────────────────────────────────────


class TestOrchestratorGold:
    def test_run_gold_step(self, tmp_path, monkeypatch):
        """Testa que o orchestrator executa o step Gold completo."""
        from config.settings import Settings
        from pipeline.orchestrator import PipelineOrchestrator

        monkeypatch.delenv("RUNTIME_ENV", raising=False)
        settings = Settings(data_root=str(tmp_path / "data"))

        # Precondição: tabela Silver conversations existe
        conv_path = settings.silver_conversations_path
        Path(conv_path).parent.mkdir(parents=True, exist_ok=True)
        backend = LocalDeltaBackend()
        backend.write_table(
            _make_silver_conversations(),
            conv_path,
            mode="overwrite",
            schema_mode="overwrite",
        )

        # Criar gold dir
        Path(settings.gold_path).mkdir(parents=True, exist_ok=True)

        orch = PipelineOrchestrator()
        # Injetar settings para usar tmp_path
        orch.settings = settings

        run = orch.run_pipeline(layers=["gold"], trigger="test")

        assert run.status.value == "completed"
        assert len(run.steps) == 1
        assert run.steps[0].step_name == "gold"
        assert run.steps[0].rows_output > 0

        # Verificar que as tabelas Gold foram criadas
        assert backend.table_exists(settings.gold_sentiment_path)
        assert backend.table_exists(settings.gold_personas_path)
        assert backend.table_exists(settings.gold_segmentation_path)
        assert backend.table_exists(settings.gold_analytics_path)
        assert backend.table_exists(settings.gold_vendor_path)


# ─── CodeGen Agent tests ─────────────────────────────────────────────────────


class TestCodeGenAgentRecommendations:
    """Testa a recomendação heurística de módulos Gold (sem LLM)."""

    def test_recommend_sentiment_from_kpis(self):
        from agents.codegen_agent import CodeGenAgent
        from pipeline.specs import ProjectSpec

        agent = CodeGenAgent()
        spec = ProjectSpec(
            descricao_kpis="Queremos análise de sentimento por conversa",
            dicionario_dados="dummy",
        )
        modulos = agent.recomendar_modulos_gold(spec)
        assert "sentiment" in modulos

    def test_recommend_personas_from_kpis(self):
        from agents.codegen_agent import CodeGenAgent
        from pipeline.specs import ProjectSpec

        agent = CodeGenAgent()
        spec = ProjectSpec(
            descricao_kpis="Classificação de personas e perfis de leads",
            dicionario_dados="dummy",
        )
        modulos = agent.recomendar_modulos_gold(spec)
        assert "personas" in modulos

    def test_recommend_all_when_generic(self):
        from agents.codegen_agent import CodeGenAgent
        from pipeline.specs import ProjectSpec

        agent = CodeGenAgent()
        spec = ProjectSpec(
            descricao_kpis="Gerar insights gerais dos dados",
            dicionario_dados="dummy",
        )
        modulos = agent.recomendar_modulos_gold(spec)
        # KPIs genéricos → retorna todos os módulos
        assert len(modulos) == 5

    def test_recommend_vendor_analysis(self):
        from agents.codegen_agent import CodeGenAgent
        from pipeline.specs import ProjectSpec

        agent = CodeGenAgent()
        spec = ProjectSpec(
            descricao_kpis="Métricas de performance do time de vendas e vendedor",
            dicionario_dados="dummy",
        )
        modulos = agent.recomendar_modulos_gold(spec)
        assert "vendor_analysis" in modulos

    def test_gold_modules_registry(self):
        from agents.codegen_agent import GOLD_MODULES

        assert "sentiment" in GOLD_MODULES
        assert "personas" in GOLD_MODULES
        assert "segmentation" in GOLD_MODULES
        assert "analytics" in GOLD_MODULES
        assert "vendor_analysis" in GOLD_MODULES
        for mod in GOLD_MODULES.values():
            assert "nome" in mod
            assert "modulo" in mod
            assert "funcao" in mod
