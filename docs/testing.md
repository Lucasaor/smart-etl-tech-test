# Testes

224 testes automatizados organizados por fase:

| Arquivo | Testes | Cobertura |
|---------|--------|-----------|
| `test_phase1.py` | 34 | Fundação: config, storage, compute, events, specs |
| `test_phase2.py` | 16 | Bronze: ingestão, orchestrator, spec integration |
| `test_phase3.py` | 33 | Silver: limpeza, extração, conversas, spec tools |
| `test_phase4.py` | 45 | Gold: sentimento, personas, segmentação, analytics, vendedores |
| `test_phase5.py` | 96 | Agentes: data tools, quality tools, pipeline/monitor/repair agents |

## Executar Testes

```bash
# Todos os testes
pytest tests/ -v

# Apenas uma fase
pytest tests/test_phase1.py -v

# Com cobertura
pytest tests/ --cov=pipeline --cov=agents --cov=core --cov=config --cov=monitoring
```
