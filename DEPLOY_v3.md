# INSTRUÇÕES DE DEPLOY – APEX PREDATOR NEO v3

## PRÉ-REQUISITOS
- Docker >= 24.0 + Docker Compose V2
- API keys Binance Spot (+ Simple Earn habilitado)
- Testnet: https://testnet.binance.vision/

---

## PASSO 1 – PREPARAR
```bash
cd ~/apex-predator-neo-v3
cp .env.example .env
nano .env
# Preencher: BINANCE_TESTNET_API_KEY e BINANCE_TESTNET_API_SECRET
# Manter: TESTNET=True
```

## PASSO 2 – BUILD
```bash
docker compose build --no-cache
docker compose config   # verificar se está tudo ok
```

## PASSO 3 – SUBIR SCANNER (TESTE 2 HORAS)
```bash
docker compose up -d redis scanner
docker compose logs -f scanner
```

### O que observar:
- ✅ "Redis conectado"
- ✅ "Binance [TESTNET]"
- ✅ "X triângulos únicos encontrados"
- ✅ Linhas com 🎯 (oportunidades)
- ✅ Heartbeats a cada 30s

### Monitoramento:
```bash
docker compose ps
docker exec apexv3_redis redis-cli subscribe apex:v3:opportunities
docker exec apexv3_redis redis-cli subscribe apex:v3:heartbeat
```

## PASSO 4 – ATIVAR EXECUTORES (APÓS 2H OK)
```bash
docker compose up -d singapore_executor tokyo_executor
docker compose logs -f
```

## PASSO 5 – MONITORAMENTO
```bash
docker stats                                                    # recursos
docker exec apexv3_redis redis-cli subscribe apex:v3:executions # trades
docker exec apexv3_redis redis-cli subscribe apex:v3:risk       # Robin Hood
docker exec apexv3_redis redis-cli subscribe apex:v3:earn       # Auto-Earn
docker exec apexv3_redis redis-cli get apex:v3:risk             # estado risco
```

## PASSO 6 – IR PARA PRODUÇÃO
```bash
docker compose down
nano .env
# Mudar: TESTNET=False
# Preencher: BINANCE_API_KEY e BINANCE_API_SECRET (produção)
docker compose up -d
docker compose logs -f   # monitorar primeiros 30min
```

## COMANDOS ÚTEIS
```bash
docker compose down                     # parar tudo
docker compose restart scanner          # reiniciar scanner
docker compose logs --since 1h scanner | grep ERROR  # erros recentes
docker compose build --no-cache && docker compose up -d  # rebuild
```

## TROUBLESHOOTING

| Problema | Solução |
|----------|---------|
| Redis não conecta | `docker compose ps redis` / verificar porta |
| Keys inválidas | Conferir .env testnet vs live |
| Zero triângulos | Testnet tem poucos pares; em live terá centenas |
| Robin Hood pausou | Normal — esperar 30min ou ajustar MAX_DRAWDOWN_PCT |
| Latência alta | VPS mais próxima da Binance (AWS ap-southeast-1) |

## ARQUITETURA
```
Scanner (Curitiba) ──→ Redis Pub/Sub (< 5ms)
                         ├──→ Singapore Executor → Binance (< 40ms)
                         └──→ Tokyo Executor     → Binance (< 60ms)
                                      │
                               Robin Hood Risk
                                      │
                               Auto-Earn Hook → Simple Earn
```

*APEX PREDATOR NEO v3 — starbot666 🦈*
