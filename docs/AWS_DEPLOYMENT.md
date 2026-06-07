# Despliegue temporal en AWS

Esta guía está orientada a una práctica universitaria que solo permanecerá
activa durante su evaluación. Prioriza un despliegue sencillo, económico y
fácil de demostrar sobre requisitos propios de producción.

```text
Exchanges
   |
   v
EC2 Collector -> Kinesis Data Streams -> Lambda Processor
                                             |       |
                                             v       v
EC2 Dashboard ----------------------> RDS PostgreSQL S3 raw ticks
```

El collector y la Lambda se comunican únicamente mediante el contrato JSON de
Kinesis. La Lambda escribe los ticks crudos en S3 y mantiene oportunidades y
últimos precios en PostgreSQL.

## Alcance y simplificaciones

Para la evaluación se recomienda:

- Una EC2 pública para el collector y otra EC2 pública para el dashboard.
- VPC predeterminada o una VPC sencilla, sin NAT Gateway.
- RDS PostgreSQL privado, Single-AZ y de tamaño mínimo.
- Conexión directa de Lambda y dashboard a RDS, sin RDS Proxy.
- Acceso al dashboard directamente por `http://<ec2-public-ip>:8501`.
- Credenciales en variables de entorno, sin Secrets Manager, siempre que no se
  suban al repositorio.
- Logs básicos de CloudWatch, sin alarmas ni paneles adicionales.

Estas simplificaciones son razonables para una ejecución breve y con poco
tráfico. Para producción serían recomendables RDS Proxy, Secrets Manager, ALB
con HTTPS, subredes privadas y observabilidad completa.

## 1. Prerrequisitos

- Una región AWS única para todos los recursos.
- AWS CLI configurada para construir y desplegar.
- Python 3.12 para coincidir con el runtime Lambda propuesto.
- Una VPC; puede utilizarse la VPC predeterminada de la cuenta.
- Tu IP pública para limitar SSH y el acceso al dashboard.

Variables usadas en los ejemplos:

```bash
export AWS_REGION=eu-west-1
export PROJECT=crypto-arbitrage
export KINESIS_STREAM=crypto-arbitrage-ticks
export RAW_BUCKET=crypto-arbitrage-raw-<account-id>
```

## 2. Red y security groups

Utiliza la misma VPC para ambas EC2, Lambda y RDS. El collector necesita IP
pública para conectarse a los exchanges. El dashboard puede usar otra IP
pública para mostrarse durante la evaluación. RDS debe permanecer privado.

Security groups mínimos:

| Security group | Entrada | Uso |
|---|---|---|
| `sg-collector` | TCP 22 únicamente desde tu IP | EC2 collector |
| `sg-dashboard` | TCP 22 y 8501 únicamente desde tu IP o la del evaluador | EC2 dashboard |
| `sg-lambda` | Ninguna | Lambda Processor |
| `sg-rds` | TCP 5432 desde `sg-dashboard` y `sg-lambda` | RDS PostgreSQL |

Mantén la salida predeterminada permitida. No abras SSH, PostgreSQL ni
Streamlit a `0.0.0.0/0`; si cambia tu IP, actualiza temporalmente las reglas.

Conecta Lambda a subredes de la VPC para alcanzar RDS. Como el código también
escribe en S3, crea un **gateway endpoint de S3** para la VPC. Es gratuito y
evita desplegar un NAT Gateway.

## 3. Kinesis Data Streams

Crea un stream llamado `crypto-arbitrage-ticks`.

Configuración inicial recomendada:

- Modo provisionado con un shard, suficiente para la práctica.
- Retención de 24 horas inicialmente.
- Cifrado administrado por AWS.

Los mensajes usan `coin` como partition key. Esto conserva el orden por moneda
y distribuye monedas diferentes entre shards.

## 4. Bucket S3

Crea un bucket privado y bloquea todo acceso público.

Ruta usada por el processor:

```text
s3://<bucket>/raw_ticks/YYYY/MM/DD/<batch-hash>.json
```

Configuración recomendada:

- Cifrado SSE-S3 predeterminado.
- Versionado desactivado.
- Regla lifecycle para eliminar objetos después de 7 días.
- Política que permita `s3:PutObject` únicamente a la Lambda Processor.

## 5. RDS PostgreSQL

1. Crea una instancia RDS PostgreSQL pequeña, Single-AZ y en la misma VPC.
2. Marca `Publicly accessible` como `No`.
3. Desactiva la protección contra borrado y usa una retención de backups corta.
4. Crea la base de datos y usuario de aplicación.
5. Ejecuta [`schema.sql`](../schema.sql).
6. Permite conexiones desde `sg-dashboard` y `sg-lambda`.

El código recibe un DSN completo apuntando directamente al endpoint RDS:

```text
postgresql://<user>:<password>@<rds-endpoint>:5432/<database>
```

Para esta práctica puede configurarse como variable de entorno de Lambda y en
el archivo local de la EC2. No incluyas el DSN en Git ni en capturas públicas.
RDS Proxy no es necesario con el bajo volumen y la corta duración previstos.

## 6. Construir las Lambdas

Desde la raíz del repositorio:

```bash
python tools/build_lambdas.py all
```

Genera artefactos Linux para Lambda Python 3.12 x86_64:

```text
dist/lambdas/poller.zip
dist/lambdas/processor.zip
```

Para ARM64:

```bash
python tools/build_lambdas.py all --platform manylinux2014_aarch64
```

### Lambda Processor

Configuración:

| Campo | Valor |
|---|---|
| Runtime | Python 3.12 |
| Handler | `crypto_arbitrage_aws.lambdas.processor.lambda_handler` |
| Memoria inicial | 512 MB |
| Timeout inicial | 60 segundos |
| VPC | Subredes con acceso a RDS y al endpoint S3 |

Variables:

```text
DB_DSN=postgresql://...@<rds-endpoint>:5432/...
S3_BUCKET=<raw-bucket>
MAX_PRICE_AGE_SECONDS=120
ARBITRAGE_THRESHOLD_PCT=0.3
```

Permisos IAM mínimos:

- Lectura de Kinesis: `kinesis:DescribeStream`, `kinesis:DescribeStreamSummary`,
  `kinesis:GetRecords`, `kinesis:GetShardIterator` y `kinesis:ListShards`.
- `s3:PutObject` sobre `arn:aws:s3:::<raw-bucket>/raw_ticks/*`.
- Permisos básicos de logs de Lambda.
- Permisos VPC administrados por Lambda.

Ejemplo de política específica del proyecto:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:DescribeStream",
        "kinesis:DescribeStreamSummary",
        "kinesis:GetRecords",
        "kinesis:GetShardIterator",
        "kinesis:ListShards"
      ],
      "Resource": "arn:aws:kinesis:<region>:<account>:stream/crypto-arbitrage-ticks"
    },
    {
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::<raw-bucket>/raw_ticks/*"
    }
  ]
}
```

Configura un event source mapping desde Kinesis:

- `Starting position`: `LATEST`.
- Batch size inicial: `100`.
- Maximum batching window inicial: `5` segundos.
- `BisectBatchOnFunctionError`: activado.
- Maximum retry attempts limitado para evitar reintentos indefinidos.

El processor es idempotente para oportunidades y raw batches, por lo que los
reintentos de Kinesis no deberían duplicar registros.

### Lambda Poller opcional

El flujo principal usa EC2 WebSocket Collector. La Lambda Poller REST puede
desplegarse como fallback:

| Campo | Valor |
|---|---|
| Handler | `crypto_arbitrage_aws.lambdas.poller.lambda_handler` |
| Variable | `KINESIS_STREAM=<stream-name>` |
| IAM | `kinesis:PutRecords` sobre el stream |
| Trigger | EventBridge Scheduler |

## 7. EC2 WebSocket Collector

### Rol IAM

Asigna un instance profile con:

- `kinesis:PutRecords` sobre el stream.

No guardes access keys en la instancia.

Política mínima del collector:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "kinesis:PutRecords",
      "Resource": "arn:aws:kinesis:<region>:<account>:stream/crypto-arbitrage-ticks"
    }
  ]
}
```

### Instalación

Ejemplo para Amazon Linux:

```bash
sudo dnf install -y git python3.12
sudo mkdir -p /opt/crypto-arbitrage
sudo chown ec2-user:ec2-user /opt/crypto-arbitrage
git clone <repository-url> /opt/crypto-arbitrage/app
cd /opt/crypto-arbitrage/app
python3.12 -m venv .venv
.venv/bin/pip install ".[collector]"
```

Archivo `/etc/crypto-arbitrage/collector.env`:

```text
AWS_REGION=eu-west-1
KINESIS_STREAM=crypto-arbitrage-ticks
BATCH_INTERVAL=30

# Overrides opcionales por restricciones geográficas:
# BINANCE_WS_URL=wss://proxy.example/stream
# KRAKEN_WS_URL=wss://...
# COINBASE_WS_URL=wss://...
# BYBIT_WS_URL=wss://...
```

Protege el archivo:

```bash
sudo mkdir -p /etc/crypto-arbitrage
sudo chmod 600 /etc/crypto-arbitrage/collector.env
```

Servicio systemd `/etc/systemd/system/crypto-arbitrage-collector.service`:

```ini
[Unit]
Description=Crypto Arbitrage WebSocket Collector
After=network-online.target
Wants=network-online.target

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

Activación:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-arbitrage-collector
sudo systemctl status crypto-arbitrage-collector
journalctl -u crypto-arbitrage-collector -f
```

## 8. Dashboard en una segunda EC2

Despliega una segunda EC2 pública pequeña, dentro de la misma VPC. Esta
instancia no necesita permisos IAM de Kinesis. Detén ambas EC2 cuando no se
estén usando para limitar el coste de la práctica.

```bash
sudo dnf install -y git python3.12
sudo mkdir -p /opt/crypto-arbitrage
sudo chown ec2-user:ec2-user /opt/crypto-arbitrage
git clone <repository-url> /opt/crypto-arbitrage/app
cd /opt/crypto-arbitrage/app
python3.12 -m venv .venv
.venv/bin/pip install ".[dashboard]"
```

Archivo `/etc/crypto-arbitrage/dashboard.env`:

```text
DB_TYPE=postgres
DB_DSN=postgresql://<user>:<password>@<rds-endpoint>:5432/<database>
REFRESH_INTERVAL=30
ARBITRAGE_THRESHOLD_PCT=0.5
```

Protege el archivo:

```bash
sudo mkdir -p /etc/crypto-arbitrage
sudo chmod 600 /etc/crypto-arbitrage/dashboard.env
```

Servicio systemd `/etc/systemd/system/crypto-arbitrage-dashboard.service`:

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

Activación:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-arbitrage-dashboard
sudo systemctl status crypto-arbitrage-dashboard
journalctl -u crypto-arbitrage-dashboard -f
```

Accede durante la evaluación mediante:

```text
http://<ec2-public-ip>:8501
```

Limita el puerto 8501 a la IP del evaluador o a la red universitaria. No se
necesitan ALB, dominio ni certificado para una demostración temporal.

## 9. Matriz de configuración

| Componente | Variable | Obligatoria | Descripción |
|---|---|---:|---|
| EC2 Collector | `KINESIS_STREAM` | Sí en AWS | Stream de salida |
| EC2 Collector | `BATCH_INTERVAL` | No | Segundos por batch; default `30` |
| EC2 Collector | `BINANCE_WS_URL` | No | Override geográfico/proxy |
| EC2 Collector | `KRAKEN_WS_URL` | No | Override geográfico/proxy |
| EC2 Collector | `COINBASE_WS_URL` | No | Override geográfico/proxy |
| EC2 Collector | `BYBIT_WS_URL` | No | Override geográfico/proxy |
| Lambda Processor | `DB_DSN` | Sí | DSN del endpoint RDS |
| Lambda Processor | `S3_BUCKET` | Sí | Bucket de raw ticks |
| Lambda Processor | `MAX_PRICE_AGE_SECONDS` | No | Frescura máxima; default `120` |
| Lambda Processor | `ARBITRAGE_THRESHOLD_PCT` | No | Spread mínimo; default `0.3` |
| EC2 Dashboard | `DB_TYPE` | Sí | Usar `postgres` |
| EC2 Dashboard | `DB_DSN` | Sí | DSN del endpoint RDS |
| EC2 Dashboard | `REFRESH_INTERVAL` | No | Refresco; default `30` |
| EC2 Dashboard | `ARBITRAGE_THRESHOLD_PCT` | No | Umbral visual; default `0.5` |

## 10. Observabilidad mínima

Para la evaluación basta con:

- Logs de Lambda Processor en CloudWatch.
- Métricas básicas de invocaciones y errores de Lambda.
- Métricas entrantes de Kinesis.
- `journalctl` para collector y dashboard.

## 11. Validación end-to-end

1. Comprueba que el collector está conectado:

   ```bash
   journalctl -u crypto-arbitrage-collector -f
   ```

2. Verifica métricas entrantes de Kinesis.
3. Verifica invocaciones exitosas de Lambda Processor.
4. Comprueba objetos bajo `raw_ticks/` en S3.
5. Consulta PostgreSQL:

   ```sql
   SELECT * FROM latest_prices ORDER BY observed_at DESC LIMIT 20;
   SELECT * FROM arbitrage_opportunities ORDER BY detected_at DESC LIMIT 20;
   ```

6. Abre `http://<ec2-public-ip>:8501` desde una IP permitida.

## 12. Orden recomendado de despliegue

1. VPC, endpoint S3 y security groups.
2. S3.
3. RDS PostgreSQL y schema.
4. Kinesis.
5. Lambda Processor y event source mapping.
6. EC2 Collector.
7. EC2 Dashboard.
8. Validación end-to-end.

## 13. Apagado tras la evaluación

Para evitar costes posteriores, elimina en este orden:

1. Event source mapping y Lambda.
2. Kinesis Data Stream.
3. RDS sin snapshot final si ya no necesitas los datos.
4. Ambas EC2 y sus volúmenes EBS.
5. Objetos y bucket S3.
6. Endpoint S3 y security groups creados para la práctica.

Comprueba también que no quedan Elastic IPs, snapshots o volúmenes sin asociar.

## Referencias AWS

- [Procesar Kinesis Data Streams con Lambda](https://docs.aws.amazon.com/lambda/latest/dg/services-kinesis-create.html)
- [Manejo de fallos en batches Kinesis/Lambda](https://docs.aws.amazon.com/lambda/latest/dg/kinesis-on-failure-destination.html)
- [Acceso de Lambda a recursos VPC](https://docs.aws.amazon.com/lambda/latest/dg/configuration-vpc.html)
- [Gateway endpoints para S3](https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html)
