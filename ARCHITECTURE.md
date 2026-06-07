# Arquitectura: Detector de Arbitraje en Crypto (AWS)

## Diagrama general

```
Modo REST:
EventBridge (cada 30s) → Lambda Poller ─────────┐
                                                  │
                                                  ▼
Modo WebSocket (fase 2):                   Kinesis Data Streams
EC2 Collector ────────────────────────────────────┘
                                                  │
                                                  ▼
EC2 Dashboard ────────> RDS PostgreSQL ← Lambda Processor
                                              │
                                              │
                                              ▼
                                         S3 (raw ticks)
```

> Para esta práctica temporal se mantienen dos EC2: una para el collector y
> otra para el dashboard. Así se aíslan la captura y la interfaz sin añadir ALB.

---

## Componentes

### EventBridge
- Temporizador que dispara el Lambda Poller cada 30 segundos.
- Equivalente a un cron job gestionado por AWS.

### Lambda Poller (`poller.py`)
- Consulta las APIs REST de los 4 exchanges en paralelo.
- Exchanges: **Binance, Kraken, Coinbase, Bybit**.
- Monedas: top 20 por market cap (excluyendo stablecoins), intersección de las disponibles en los 4 exchanges.
- Publica un mensaje JSON por moneda por exchange en Kinesis.

### EC2 — WebSocket Collector *(Fase 2)*
- Proceso Python con `asyncio` que mantiene conexiones WebSocket abiertas contra los exchanges.
- Produce el mismo schema JSON que el Lambda Poller.
- Se despliega separado del dashboard para aislar la captura.

### Kinesis Data Streams
- Bus de mensajes entre los productores (Poller / Collector) y el procesador.
- Desacopla la ingesta del procesamiento.

### Lambda Processor (`processor.py`)
- Se dispara automáticamente por cada batch de mensajes en Kinesis.
- Agrupa los precios por moneda dentro de una ventana de tiempo.
- Detecta oportunidades de arbitraje: diferencia de precio entre exchanges > umbral (0.3% por defecto).
- Escribe en RDS las oportunidades detectadas.
- Escribe en S3 todos los ticks crudos recibidos.

### RDS — PostgreSQL
- Persistencia de las oportunidades de arbitraje detectadas.
- La práctica conecta directamente a RDS; RDS Proxy queda como mejora de producción.

### S3
- Data lake con todos los ticks de precios en crudo (formato JSON, particionado por fecha).
- Fuente de datos para Athena.

### Athena
- Permite hacer consultas SQL ad hoc sobre los datos históricos de S3.
- Es opcional para la evaluación y no forma parte del despliegue mínimo.

### EC2 — Streamlit Dashboard
- Aplicación web ligera que lee de RDS y muestra en tiempo real:
  - Oportunidades detectadas (con timestamp, par, exchanges, spread).
  - Métricas del pipeline.

---

## Schema JSON (contrato de Kinesis)

Todos los productores (Lambda Poller y EC2 WebSocket Collector) deben generar
mensajes con exactamente este formato:

```json
{
  "timestamp": "2026-06-05T10:30:00.123Z",
  "source_mode": "rest",
  "exchange": "binance",
  "coin": "BTC",
  "price_usd": 67000.50
}
```

| Campo         | Tipo    | Valores posibles                          |
|---------------|---------|-------------------------------------------|
| `timestamp`   | string  | ISO 8601 UTC                              |
| `source_mode` | string  | `"rest"` \| `"websocket"`                |
| `exchange`    | string  | `"binance"` \| `"kraken"` \| `"coinbase"` \| `"bybit"` |
| `coin`        | string  | Símbolo en mayúsculas, ej. `"BTC"`        |
| `price_usd`   | float   | Precio en USD (o USDT tratado como USD)   |

El contrato se genera y valida en `crypto_arbitrage_aws.contracts`. Las
fronteras entre servicios usan únicamente dicts serializables y JSON/bytes de
Kinesis; las clases de los clientes WebSocket no forman parte del contrato.

---

## Schema de RDS — Tabla `arbitrage_opportunities`

```sql
CREATE TABLE arbitrage_opportunities (
    id            SERIAL PRIMARY KEY,
    opportunity_key VARCHAR(64) UNIQUE,
    detected_at   TIMESTAMPTZ NOT NULL,
    coin          VARCHAR(20) NOT NULL,
    exchange_low  VARCHAR(20) NOT NULL,
    exchange_high VARCHAR(20) NOT NULL,
    price_low     NUMERIC(20, 8) NOT NULL,
    price_high    NUMERIC(20, 8) NOT NULL,
    spread_pct    NUMERIC(8, 4) NOT NULL,
    source_mode   VARCHAR(10) NOT NULL
);
```

El processor mantiene además `latest_prices`, con el último tick por
`coin + exchange`. Esto permite detectar arbitraje aunque los precios lleguen
en batches distintos. Solo se consideran precios con menos de
`MAX_PRICE_AGE_SECONDS` segundos.

Cada oportunidad incluye una clave determinista derivada de los ticks mínimo y
máximo usados. El índice único sobre esa clave hace idempotentes los reintentos
del processor.

En PostgreSQL, el processor adquiere un bloqueo transaccional por moneda antes
de actualizar el snapshot. Así dos batches concurrentes de la misma moneda no
pueden omitir una oportunidad al leer estados parciales.

---

## Archivos del proyecto

| Ruta                                      | Descripción                                           |
|-------------------------------------------|-------------------------------------------------------|
| `src/crypto_arbitrage_aws/poller.py`      | Fetcha precios vía REST API de los 4 exchanges        |
| `src/crypto_arbitrage_aws/processor.py`   | Detecta arbitraje y persiste en RDS/SQLite + S3/disco |
| `src/crypto_arbitrage_aws/ws_collector.py`| Alternativa WebSocket en tiempo real (EC2, Fase 2)    |
| `src/crypto_arbitrage_aws/run_local.py`   | Simula EventBridge + Lambda en local (bucle REST)     |
| `src/crypto_arbitrage_aws/dashboard.py`   | Dashboard Streamlit con autorefresh                   |
| `src/crypto_arbitrage_aws/scripts/`        | Endpoints CLI; contiene todos los `main()`            |
| `pyproject.toml`                          | Metadatos, dependencias y comandos del paquete        |
| `schema.sql`                              | Schema PostgreSQL para inicializar RDS en AWS         |

El collector WebSocket usa una clase base para gestionar conexión, reconexión
y decodificación. Cada exchange implementa únicamente su URL, suscripción y
normalización, y emite dicts que cumplen el contrato JSON común.

Los endpoints WebSocket pueden sustituirse mediante variables de entorno,
manteniendo los endpoints públicos actuales como valor por defecto:

```bash
BINANCE_WS_URL=wss://proxy.example/stream crypto-arbitrage-ws
```

Variables disponibles: `BINANCE_WS_URL`, `KRAKEN_WS_URL`,
`COINBASE_WS_URL` y `BYBIT_WS_URL`. Para Binance también puede utilizarse una
plantilla con `{streams}`, por ejemplo
`BINANCE_WS_URL='wss://proxy.example/{streams}'`.

## Desarrollo local

Instala el paquete en modo editable:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[local,dev]"
```

Comandos disponibles:

```bash
crypto-arbitrage             # pipeline REST local en bucle
crypto-arbitrage-poller      # prueba del poller REST
crypto-arbitrage-processor   # pipeline REST completo una vez
crypto-arbitrage-ws          # collector WebSocket
crypto-arbitrage-dashboard   # dashboard Streamlit
```

Los comandos se resuelven a funciones `main()` dentro de
`crypto_arbitrage_aws.scripts`. Los módulos de dominio no contienen lógica de
arranque y se pueden importar sin ejecutar procesos.

Ejecuta los tests unitarios con:

```bash
python -m pytest
```

Cada servicio se puede instalar con sus dependencias aisladas:

```bash
python -m pip install ".[poller]"      # Lambda Poller
python -m pip install ".[processor]"   # Lambda Processor
python -m pip install ".[collector]"   # EC2 WebSocket Collector
python -m pip install ".[dashboard]"   # EC2 Streamlit Dashboard
```

Los handlers para Lambda son:

```text
crypto_arbitrage_aws.lambdas.poller.lambda_handler
crypto_arbitrage_aws.lambdas.processor.lambda_handler
```

Los módulos `poller.py` y `processor.py` contienen dominio reutilizable y no
conocen eventos Lambda. Los adaptadores de `crypto_arbitrage_aws.lambdas`
validan configuración AWS obligatoria y nunca usan SQLite ni almacenamiento
local.

Los ZIPs independientes se generan con:

```bash
python tools/build_lambdas.py all
```

Consulta `deploy/lambdas/README.md` para variables, IAM y arquitectura del
runtime.

La guía completa del pipeline EC2 Collector → Kinesis → Lambda Processor →
RDS/S3, incluyendo la estrategia recomendada para el dashboard, está en
[`docs/AWS_DEPLOYMENT.md`](docs/AWS_DEPLOYMENT.md).

## Plan de desarrollo

| Fase | Componente              | Estado  |
|------|-------------------------|---------|
| 1    | Lambda Poller           | ✅ Listo |
| 1    | Lambda Processor        | ✅ Listo |
| 1    | Schema RDS              | ✅ Listo |
| 1    | Pipeline end-to-end     | ✅ Listo |
| 2    | EC2 WebSocket Collector | ✅ Listo |
| 2    | Streamlit Dashboard     | ✅ Listo |
| 3    | Despliegue en AWS       | Pendiente|

---

## Decisiones de diseño

- **REST sobre WebSocket (Fase 1)**: menor volumen de datos, más fácil de controlar el gasto en AWS Academy.
- **PostgreSQL sobre MariaDB**: preferencia del equipo. La práctica conecta
  directamente a RDS para evitar desplegar RDS Proxy.
- **Dos EC2 durante la evaluación**: collector y dashboard se separan para
  evitar que la interfaz afecte a la captura, pero se omite ALB para simplificar.
- **Schema común en Kinesis**: desacopla el modo de ingesta del procesamiento, permite añadir fuentes nuevas sin tocar el downstream.
