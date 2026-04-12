# KPIs e Análises — Camada Gold

Indicadores e análises gerados automaticamente na camada Gold a partir dos dados transacionais de vendas de seguro automotivo via WhatsApp.

## 1. Funil de Conversão

Distribuição das conversas por resultado final (`conversation_outcome`).

| Métrica | Descrição |
|---|---|
| Total de conversas por outcome | Contagem e percentual de cada outcome |
| Taxa de conversão geral | % de conversas com outcome `venda_fechada` sobre o total |
| Taxa de propostas enviadas | % de conversas onde chegou-se a enviar proposta |
| Taxa de perda | % de conversas perdidas para preço ou concorrente |

## 2. Lead Scoring

Score de 0 a 100 por conversa baseado em sinais comportamentais:

| Componente | Peso | Descrição |
|---|---|---|
| Engajamento | 30 pts | Proporção de mensagens inbound sobre o total |
| Velocidade de resposta | 25 pts | Baseado no `response_time_sec` médio |
| Dados compartilhados (PII) | 20 pts | Se o lead compartilhou CPF, e-mail e/ou telefone |
| Profundidade da conversa | 15 pts | Faixas baseadas em `total_messages` |
| Veículo mencionado | 10 pts | Se há placa ou dados do veículo identificados |

## 3. Performance por Campanha

Métricas agregadas por `campaign_id`:

- Total de conversas por campanha
- Vendas fechadas e taxa de conversão (%) por campanha
- Média de mensagens por conversa em cada campanha
- Tempo médio de resposta dos leads por campanha
- Lead score médio por campanha
- Ranking de campanhas por taxa de conversão

## 4. Performance por Vendedor

Métricas agregadas por `agent_id`:

- Total de conversas atendidas
- Vendas fechadas e taxa de conversão (%)
- Taxa de ghosting (%)
- Média de mensagens por conversa
- Tempo médio de resposta de seus leads
- Score de eficiência combinando taxa de conversão e duração média
- Ranking geral dos vendedores

## 5. Classificação de Personas

| Persona | Descrição | Critérios principais |
|---|---|---|
| **Decidido** | Fecha rápido, conversa curta | outcome=venda_fechada + ≤12 mensagens |
| **Pesquisador** | Faz muitas perguntas, compara | 8-15 mensagens, sem fechamento |
| **Negociador** | Vai e volta, pede desconto | >15 mensagens ou menção a concorrente |
| **Fantasma** | Para de responder | outcome=ghosting + ≤3 mensagens inbound |
| **Indeciso** | Sem decisão clara | demais casos |

## 6. Análise de Sentimento

Score de sentimento por conversa, de -1 (muito negativo) a +1 (muito positivo):

| Componente | Peso | Descrição |
|---|---|---|
| Outcome da conversa | 50% | venda_fechada → positivo, ghosting → negativo |
| Engajamento do lead | 20% | Proporção de mensagens inbound |
| Velocidade de resposta | 15% | Respostas rápidas indicam interesse positivo |
| Profundidade da conversa | 15% | Conversas mais longas sugerem engajamento |

Classificação final: **positivo** (score > 0.2), **neutro** (-0.2 a 0.2), **negativo** (< -0.2).

## 7. Segmentação Multidimensional

| Dimensão | Segmentos | Baseado em |
|---|---|---|
| **Engajamento** | alto / médio / baixo | total_messages (≥15, ≥6, <6) |
| **Velocidade de resposta** | rápido / moderado / lento / sem_dados | avg_response_time_sec |
| **Veículo** | com_veiculo / sem_veiculo | Presença de placa identificada |
| **Região** | sudeste / sul / nordeste / centro_oeste / norte | UF do lead |
| **Horário** | comercial / fora_horario | is_business_hours dos metadados |
| **Origem** | Por campanha e lead_source | campaign_id e metadata.lead_source |

## 8. Análise Temporal

- Volume diário de novas conversas
- Vendas por dia da semana e por faixa horária
- Tempo médio de fechamento (primeira mensagem → venda_fechada)
- Picos de demanda — dias e horários com maior volume de inbound

## 9. Análise de Concorrência

- Frequência de menção a cada concorrente (Porto Seguro, Azul, Bradesco Seguros, SulAmérica, Liberty, Allianz)
- Taxa de perda quando concorrente é mencionado vs. não mencionado
- Concorrentes mais citados por faixa de preço/região
- Relação entre menção a concorrente e outcome da conversa
