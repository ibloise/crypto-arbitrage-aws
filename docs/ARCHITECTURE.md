# Arquitectura y despliegue en AWS

## Diagrama general

```
Modo REST:
EventBridge (cada minuto) → Lambda Poller ──────┐
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

| Componente | Responsabilidad |
|---|---|
| EventBridge + Lambda Poller | Consulta por REST Binance, Kraken, Coinbase y Bybit y publica ticks en Kinesis. |
| EC2 Collector | Mantiene WebSockets con los exchanges y publica el mismo contrato JSON. |
| Kinesis Data Streams | Desacopla productores y Processor. |
| Lambda Processor | Detecta arbitraje, persiste resultados en RDS y guarda ticks crudos en S3. |
| RDS PostgreSQL | Conserva últimos precios y oportunidades detectadas. |
| EC2 Dashboard | Lee RDS y muestra oportunidades y métricas mediante Streamlit. |

Athena puede consultar los datos históricos de S3, pero es opcional para la
evaluación.

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

## Persistencia e idempotencia

El schema completo está en [`schema.sql`](../schema.sql). El Processor mantiene
`latest_prices`, con el último tick por `coin + exchange`, y solo usa precios
con menos de `MAX_PRICE_AGE_SECONDS` segundos.

Cada oportunidad usa una clave determinista con índice único para soportar
reintentos. En PostgreSQL, un bloqueo transaccional por moneda evita lecturas
parciales entre batches concurrentes.

---

## Decisiones de diseño

- **PostgreSQL sobre MariaDB**: preferencia del equipo. La práctica conecta
  directamente a RDS para evitar desplegar RDS Proxy.
- **Dos EC2 durante la evaluación**: collector y dashboard se separan para
  evitar que la interfaz afecte a la captura, pero se omite ALB para simplificar.
- **Schema común en Kinesis**: desacopla el modo de ingesta del procesamiento, permite añadir fuentes nuevas sin tocar el downstream.

---

## Despliegue temporal en AWS

### Alcance y simplificaciones

- Una EC2 pública para el collector y otra para el dashboard.
- VPC predeterminada o sencilla.
- RDS PostgreSQL privado, Single-AZ y de tamaño mínimo.
- Conexión directa de Lambda y dashboard a RDS, sin RDS Proxy.
- Dashboard accesible mediante `http://<ec2-public-ip>:8501`.
- Credenciales en variables de entorno no versionadas.
- Logs básicos de CloudWatch, sin observabilidad avanzada.

### Prerrequisitos

- Una única región AWS para todos los recursos.
- AWS CLI configurada.
- Python 3.12 para construir las Lambdas.
- Una VPC; puede utilizarse la predeterminada.
- Tu IP pública.
- En AWS Academy, acceso a `LabRole` para las Lambdas y a
  `LabInstanceProfile` para las instancias EC2.

### Red y security groups

Utiliza la misma VPC para ambas EC2, Lambda y RDS. RDS debe permanecer privado.

| Security group | Entrada | Uso |
|---|---|---|
| `sg-collector` | TCP 22 únicamente desde tu IP | EC2 Collector |
| `sg-dashboard` | TCP 22 y 8501 desde IPs autorizadas | EC2 Dashboard |
| `sg-lambda` | Ninguna | Lambdas Init DB y Processor |
| `sg-rds` | TCP 5432 desde `sg-dashboard` y `sg-lambda` | RDS PostgreSQL |

Mantén la salida predeterminada permitida. No abras SSH, PostgreSQL ni
Streamlit a `0.0.0.0/0`.

Conecta Lambda a la VPC para alcanzar RDS y crea un gateway endpoint de S3.

### Kinesis Data Streams

Crea `market-quotes` con:

- Modo provisionado y un shard.
- Retención de 24 horas.
- Cifrado administrado por AWS.

Los productores utilizan `coin` como partition key, conservando el orden por
moneda.

### S3

Crea un bucket privado con acceso público bloqueado. El processor escribe:

```text
s3://<bucket>/raw_ticks/YYYY/MM/DD/<batch-hash>.json
```


### RDS PostgreSQL

1. Crea una instancia pequeña, Single-AZ y privada en la misma VPC.
2. Desactiva protección contra borrado y usa una retención de backups corta.
3. Crea la base de datos y el usuario de aplicación.
4. Permite conexiones desde `sg-dashboard` y `sg-lambda`.
5. Invoca Lambda Init DB para crear el schema.

Los servicios reciben la conexión mediante una interfaz común:

```text
DB_TYPE=postgres
DB_HOST=<rds-endpoint>
DB_PORT=5432
DB_NAME=postgres
DB_USER=<database-user>
DB_PASSWORD=<database-password>
```

Puede almacenarse en las variables de entorno de Lambda y en el archivo
privado de la EC2 Dashboard. No incluyas las credenciales en Git.

### Construcción de Lambdas

Desde la raíz:

```bash
./scripts/build_lambdas.sh
```

Genera artefactos Linux para Lambda Python 3.12 x86_64:

```text
dist/lambdas/init-db.zip
dist/lambdas/poller.zip
dist/lambdas/processor.zip
```

Para ARM64:

```bash
./scripts/build_lambdas.sh all --platform manylinux2014_aarch64
```

#### Lambda Init DB

Esta Lambda inicializa RDS PostgreSQL después de crear la instancia. Se invoca
manualmente una vez y puede repetirse porque todas las operaciones del schema
son idempotentes.

| Campo | Valor |
|---|---|
| Runtime | Python 3.12 |
| Handler | `crypto_arbitrage_aws.lambdas.init_db.lambda_handler` |
| Timeout inicial | 30 segundos |
| VPC | Subredes con acceso a RDS |
| Trigger | Ninguno; invocación manual |

Variables:

```text
DB_TYPE=postgres
DB_HOST=<rds-endpoint>
DB_PORT=5432
DB_NAME=postgres
DB_USER=<database-user>
DB_PASSWORD=<database-password>
```

Después de subir `init-db.zip`, abre **Runtime settings > Edit** y sustituye el
handler predeterminado por:

```text
crypto_arbitrage_aws.lambdas.init_db.lambda_handler
```

La Lambda no podrá resolver ni conectar con un RDS privado solo con
`DB_HOST`. En **Configuration > VPC**, asígnale la misma VPC que RDS, subredes
con ruta hacia RDS y `sg-lambda`. Configura además `sg-rds` para aceptar TCP
5432 desde `sg-lambda`. Añade las variables anteriores en **Configuration >
Environment variables** antes de ejecutar el test manual.

En AWS Academy puede utilizarse `LabRole` como execution role. La función no
necesita permisos IAM sobre Kinesis o S3. Tras una ejecución exitosa, puede
eliminarse para reducir recursos temporales.

#### Lambda Processor

| Campo | Valor |
|---|---|
| Runtime | Python 3.12 |
| Handler | `crypto_arbitrage_aws.lambdas.processor.lambda_handler` |
| Memoria inicial | 512 MB |
| Timeout inicial | 60 segundos |
| VPC | Subredes con acceso a RDS y al endpoint S3 |

Variables:

```text
DB_TYPE=postgres
DB_HOST=<rds-endpoint>
DB_PORT=5432
DB_NAME=postgres
DB_USER=<database-user>
DB_PASSWORD=<database-password>
S3_BUCKET=<raw-bucket>
MAX_PRICE_AGE_SECONDS=120
ARBITRAGE_THRESHOLD_PCT=0.3
```

Después de subir `processor.zip`, configura **Runtime settings > Handler** con:

```text
crypto_arbitrage_aws.lambdas.processor.lambda_handler
```

Igual que Init DB, Processor debe asociarse en **Configuration > VPC** a la
VPC, subredes y `sg-lambda` que permiten alcanzar RDS. Carga también todas las
variables anteriores en **Configuration > Environment variables**.

Su rol necesita permisos básicos de logs y VPC, lectura del stream Kinesis y
`s3:PutObject` sobre `arn:aws:s3:::<raw-bucket>/raw_ticks/*`.

Configura el event source mapping con:

- Starting position `LATEST`.
- Batch size `100`.
- Maximum batching window `5` segundos.
- `BisectBatchOnFunctionError` activado.
- Número máximo de reintentos limitado.

#### Lambda Poller opcional

El flujo principal utiliza el collector WebSocket. El Poller REST puede
desplegarse como fallback:

| Campo | Valor |
|---|---|
| Handler | `crypto_arbitrage_aws.lambdas.poller.lambda_handler` |
| Variable | `KINESIS_STREAM=<stream-name>` |
| IAM | `kinesis:PutRecords` sobre el stream |
| Trigger | EventBridge Scheduler |

Después de subir `poller.zip`, establece el handler:

```text
crypto_arbitrage_aws.lambdas.poller.lambda_handler
```

Endpoints REST opcionales:

```text
COINGECKO_REST_URL=https://api.coingecko.com/api/v3
BINANCE_REST_URL=https://api.binance.com
KRAKEN_REST_URL=https://api.kraken.com
COINBASE_PRODUCTS_REST_URL=https://api.exchange.coinbase.com
COINBASE_PRICE_REST_URL=https://api.coinbase.com
BYBIT_REST_URL=https://api.bybit.com
ENABLED_EXCHANGES=binance,kraken,coinbase,bybit
LOG_LEVEL=INFO
```

Los valores mostrados son los defaults. Pueden apuntarse a proxies o endpoints
alternativos sin modificar el código. Si CoinGecko falla se utiliza un universo
local de respaldo; si falla la disponibilidad o el precio de un exchange, el
Poller continúa publicando ticks de los proveedores restantes.
`ENABLED_EXCHANGES` permite excluir proveedores antes de realizar peticiones.
Con `LOG_LEVEL=DEBUG`, el Poller registra URL final, parámetros, duración,
status HTTP y errores de respuesta.

Actualmente existe una incidencia conocida con Binance: según la región desde
la que se ejecute Lambda, el endpoint REST predeterminado puede estar
restringido o devolver HTTP 451. Mientras se diagnostica, configura las
variables de entorno de la Lambda con un endpoint Binance accesible o excluye
Binance:

```text
ENABLED_EXCHANGES=kraken,coinbase,bybit
LOG_LEVEL=DEBUG
```

EventBridge solo dispara la Lambda; `ENABLED_EXCHANGES` y los overrides
`*_REST_URL` se configuran en **Lambda > Configuration > Environment
variables**. Para la regla de EventBridge Scheduler utiliza una expresión como
`rate(1 minute)` y selecciona Lambda Poller como target.

### EC2 Collector

Al crear la EC2 en AWS Academy, abre **Advanced details > IAM instance
profile** y selecciona `LabInstanceProfile`. Si la instancia ya existe,
selecciónala y usa **Actions > Security > Modify IAM role** para asociarlo.
Esto proporciona credenciales temporales al SDK de AWS y permite publicar en
Kinesis sin guardar access keys en la instancia.

Instalación en Amazon Linux:

```bash
sudo dnf install -y git python3.12
sudo mkdir -p /opt/crypto-arbitrage
sudo chown ec2-user:ec2-user /opt/crypto-arbitrage
git clone https://github.com/ibloise/crypto-arbitrage-aws.git /opt/crypto-arbitrage/app
cd /opt/crypto-arbitrage/app
python3.12 -m venv .venv
.venv/bin/pip install ".[collector]"
```

En la EC2 configura la región, el stream y el endpoint alternativo de Binance.
El endpoint WebSocket predeterminado de Binance también puede estar bloqueado
por restricciones geográficas en AWS:

```bash
export AWS_DEFAULT_REGION=us-east-1
export AWS_REGION=us-east-1
export KINESIS_STREAM=market-quotes
export BINANCE_WS_URL="wss://data-stream.binance.vision/stream"
```

Para conservarlas después de cerrar sesión o reiniciar la instancia, añádelas
al final de `~/.bashrc` y recarga la sesión:

```bash
cat >> ~/.bashrc <<'EOF'
export AWS_DEFAULT_REGION=us-east-1
export AWS_REGION=us-east-1
export KINESIS_STREAM=market-quotes
export BINANCE_WS_URL="wss://data-stream.binance.vision/stream"
export LOG_LEVEL=INFO
EOF
source ~/.bashrc
```

Los clientes WebSocket se reconectan de forma independiente. Mensajes
malformados y errores de publicación de batches se registran y aíslan para que
no detengan los demás exchanges. Usa `LOG_LEVEL=DEBUG` para incluir trazas de
excepción y payloads de suscripción.

Para probar el collector manualmente antes de instalar el servicio:

```bash
cd /opt/crypto-arbitrage/app
source .venv/bin/activate
crypto-arbitrage-ws
```

Si se usa el servicio `systemd` descrito a continuación, crea también su
archivo de entorno, ya que `systemd` no carga automáticamente `~/.bashrc`:

```bash
sudo mkdir -p /etc/crypto-arbitrage
sudo tee /etc/crypto-arbitrage/collector.env >/dev/null <<'EOF'
AWS_DEFAULT_REGION=us-east-1
AWS_REGION=us-east-1
KINESIS_STREAM=market-quotes
BINANCE_WS_URL=wss://data-stream.binance.vision/stream
LOG_LEVEL=INFO
EOF
```

Servicio `/etc/systemd/system/crypto-arbitrage-collector.service`:

```ini
[Unit]
Description=Crypto Arbitrage WebSocket Collector
After=network-online.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/opt/crypto-arbitrage/app
EnvironmentFile=/etc/crypto-arbitrage/collector.env
ExecStart=/opt/crypto-arbitrage/app/.venv/bin/crypto-arbitrage-ws
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-arbitrage-collector
journalctl -u crypto-arbitrage-collector -f
```

### EC2 Dashboard

La segunda EC2 no necesita permisos IAM de Kinesis. En su security group de
AWS añade una regla de entrada **Custom TCP**, puerto `8501`, cuyo origen sea
tu IP y, si hace falta, la IP del evaluador. No es necesario abrir PostgreSQL
al público: `sg-rds` debe permitir el puerto 5432 desde `sg-dashboard`.

```bash
sudo dnf install -y git python3.12
sudo mkdir -p /opt/crypto-arbitrage
sudo chown ec2-user:ec2-user /opt/crypto-arbitrage
git clone https://github.com/ibloise/crypto-arbitrage-aws.git /opt/crypto-arbitrage/app
cd /opt/crypto-arbitrage/app
python3.12 -m venv .venv
.venv/bin/pip install ".[dashboard]"
```

Configura las variables de conexión en la sesión:

```bash
export DB_TYPE=postgres
export DB_HOST=<rds-endpoint>
export DB_PORT=5432
export DB_NAME=postgres
export DB_USER=<database-user>
export DB_PASSWORD=<database-password>
export REFRESH_INTERVAL=30
export ARBITRAGE_THRESHOLD_PCT=0.5
```

Añade las mismas líneas al final de `~/.bashrc` para hacerlas persistentes y
cárgalas. Sustituye primero los placeholders:

```bash
cat >> ~/.bashrc <<'EOF'
export DB_TYPE=postgres
export DB_HOST=<rds-endpoint>
export DB_PORT=5432
export DB_NAME=postgres
export DB_USER=<database-user>
export DB_PASSWORD=<database-password>
export REFRESH_INTERVAL=30
export ARBITRAGE_THRESHOLD_PCT=0.5
EOF
source ~/.bashrc
```

Arranca Streamlit escuchando en todas las interfaces de la EC2:

```bash
cd /opt/crypto-arbitrage/app
source .venv/bin/activate
crypto-arbitrage-dashboard --server.address=0.0.0.0 --server.port=8501
```

Si se usa `systemd`, crea también su archivo de entorno antes de arrancar el
servicio; los servicios no cargan `.bashrc`:

```bash
sudo mkdir -p /etc/crypto-arbitrage
sudo tee /etc/crypto-arbitrage/dashboard.env >/dev/null <<'EOF'
DB_TYPE=postgres
DB_HOST=<rds-endpoint>
DB_PORT=5432
DB_NAME=postgres
DB_USER=<database-user>
DB_PASSWORD=<database-password>
REFRESH_INTERVAL=30
ARBITRAGE_THRESHOLD_PCT=0.5
EOF
sudo chmod 600 /etc/crypto-arbitrage/dashboard.env
```

Servicio `/etc/systemd/system/crypto-arbitrage-dashboard.service`:

```ini
[Unit]
Description=Crypto Arbitrage Streamlit Dashboard
After=network-online.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/opt/crypto-arbitrage/app
EnvironmentFile=/etc/crypto-arbitrage/dashboard.env
ExecStart=/opt/crypto-arbitrage/app/.venv/bin/crypto-arbitrage-dashboard --server.address=0.0.0.0 --server.port=8501
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-arbitrage-dashboard
journalctl -u crypto-arbitrage-dashboard -f
```

Accede mediante `http://<ec2-public-ip>:8501` desde una IP permitida.

### Configuración

| Componente | Variable | Obligatoria | Descripción |
|---|---|---:|---|
| EC2 Collector | `KINESIS_STREAM` | Sí en AWS | Stream de salida |
| EC2 Collector | `BATCH_INTERVAL` | No | Segundos por batch; default `30` |
| EC2 Collector | `*_WS_URL` | No | Overrides geográficos de WebSocket |
| EC2 Collector | `WS_*_TIMEOUT` | No | Timeouts de conexión y salud WebSocket |
| EC2 Collector | `BATCH_PROCESSING_TIMEOUT` | No | Límite de publicación/proceso por batch |
| Lambda Poller | `*_REST_URL` | No | Overrides de endpoints REST |
| Poller/Collector | `LOG_LEVEL` | No | `INFO` por defecto; usar `DEBUG` para diagnóstico |
| Servicios PostgreSQL | `DB_TYPE` | Sí en AWS | Usar `postgres` |
| Servicios PostgreSQL | `DB_HOST` | Sí | Endpoint RDS |
| Servicios PostgreSQL | `DB_PORT` | No | Puerto; default `5432` |
| Servicios PostgreSQL | `DB_NAME` | No | Base de datos; default `postgres` |
| Servicios PostgreSQL | `DB_USER` | Sí | Usuario PostgreSQL |
| Servicios PostgreSQL | `DB_PASSWORD` | Sí | Contraseña PostgreSQL |
| Lambda Processor | `S3_BUCKET` | Sí | Bucket de raw ticks |
| Lambda Processor | `MAX_PRICE_AGE_SECONDS` | No | Frescura máxima; default `120` |
| Lambda Processor | `ARBITRAGE_THRESHOLD_PCT` | No | Spread mínimo; default `0.3` |
| EC2 Dashboard | `REFRESH_INTERVAL` | No | Refresco; default `30` |

### Validación end-to-end

1. Comprueba los logs del collector con `journalctl`.
2. Verifica métricas entrantes de Kinesis.
3. Verifica invocaciones exitosas de Lambda Processor.
4. Comprueba objetos bajo `raw_ticks/` en S3.
5. Consulta `latest_prices` y `arbitrage_opportunities` en PostgreSQL.
6. Abre el dashboard desde una IP permitida.

### Orden de despliegue

1. VPC, endpoint S3 y security groups.
2. S3.
3. RDS PostgreSQL.
4. Kinesis.
5. Invocación manual de Init DB.
6. Lambda Processor y event source mapping.
7. EC2 Collector.
8. EC2 Dashboard.
9. Validación end-to-end.

### Apagado tras la evaluación

1. Elimina event source mapping y Lambdas.
2. Elimina Kinesis Data Stream.
3. Elimina RDS sin snapshot final si no necesitas los datos.
4. Elimina ambas EC2 y sus volúmenes EBS.
5. Vacía y elimina el bucket S3.
6. Elimina endpoint S3 y security groups de la práctica.

Comprueba que no quedan Elastic IPs, snapshots o volúmenes sin asociar.

### Referencias AWS

- [Procesar Kinesis Data Streams con Lambda](https://docs.aws.amazon.com/lambda/latest/dg/services-kinesis-create.html)
- [Manejo de fallos en batches Kinesis/Lambda](https://docs.aws.amazon.com/lambda/latest/dg/kinesis-on-failure-destination.html)
- [Acceso de Lambda a recursos VPC](https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc.html)
- [Gateway endpoints para S3](https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html)
