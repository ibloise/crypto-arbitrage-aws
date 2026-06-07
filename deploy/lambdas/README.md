# Despliegue de Lambdas

Construye los tres artefactos aislados para AWS Lambda Python 3.12 `x86_64`:

```bash
./scripts/build_lambdas.sh
```

Para ARM64:

```bash
./scripts/build_lambdas.sh all --platform manylinux2014_aarch64
```

Los artefactos se escriben en `dist/lambdas/`.

También puede construirse un único servicio:

```bash
./scripts/build_lambdas.sh init-db
./scripts/build_lambdas.sh poller
./scripts/build_lambdas.sh processor
```

El script acepta `PYTHON_BIN` para seleccionar el intérprete y reenvía el resto
de argumentos a `tools/build_lambdas.py`.

## Init DB

- ZIP: `init-db.zip`
- Handler: `crypto_arbitrage_aws.lambdas.init_db.lambda_handler`
- Variables obligatorias: `DB_TYPE=postgres`, `DB_HOST`, `DB_USER`, `DB_PASSWORD`
- Variables opcionales: `DB_PORT` (default: `5432`), `DB_NAME` (default: `postgres`)
- Trigger: invocación manual después de crear RDS
- Red: VPC/subredes/security group con acceso TCP 5432 a RDS

La función crea el schema de forma idempotente y puede invocarse de nuevo.
Después de subir el ZIP, ajusta **Runtime settings > Handler** al valor
indicado. Configura también la VPC de RDS, subredes, security group y todas las
variables `DB_*`; definir únicamente `DB_HOST` no permite alcanzar un RDS
privado. No necesita permisos IAM de Kinesis o S3.

## Poller

- ZIP: `poller.zip`
- Handler: `crypto_arbitrage_aws.lambdas.poller.lambda_handler`
- Variable obligatoria: `KINESIS_STREAM`
- Variables opcionales: `COINGECKO_REST_URL`, `BINANCE_REST_URL`,
  `KRAKEN_REST_URL`, `COINBASE_PRODUCTS_REST_URL`,
  `COINBASE_PRICE_REST_URL`, `BYBIT_REST_URL`, `ENABLED_EXCHANGES`, `LOG_LEVEL`
- IAM: `kinesis:PutRecords` sobre el stream de destino
- Trigger: EventBridge Scheduler

Las URLs públicas actuales son los valores por defecto. Si un proveedor no
responde, el Poller continúa con los exchanges disponibles.
`ENABLED_EXCHANGES=kraken,coinbase,bybit` permite excluir proveedores
restringidos antes de realizar peticiones.
Usa `LOG_LEVEL=DEBUG` temporalmente para registrar URL final, parámetros,
duración y status HTTP de cada petición.

Existe una incidencia conocida con Binance en algunas regiones AWS: su endpoint
REST predeterminado puede estar restringido o devolver HTTP 451. Configura
`BINANCE_REST_URL` con un endpoint accesible o excluye Binance mediante
`ENABLED_EXCHANGES`. Estas variables pertenecen a la configuración de Lambda;
EventBridge únicamente programa sus invocaciones.

## Processor

- ZIP: `processor.zip`
- Handler: `crypto_arbitrage_aws.lambdas.processor.lambda_handler`
- Variables obligatorias: `DB_TYPE=postgres`, `DB_HOST`, `DB_USER`,
  `DB_PASSWORD`, `S3_BUCKET`
- Variables opcionales: `DB_PORT` (default: `5432`), `DB_NAME` (default: `postgres`)
- Variable opcional: `MAX_PRICE_AGE_SECONDS` (default: `120`)
- Variable opcional: `ARBITRAGE_THRESHOLD_PCT` (default: `0.3`)
- IAM: lectura de Kinesis y `s3:PutObject` sobre `s3://<bucket>/raw_ticks/*`
- Trigger: Kinesis event source mapping
- Red: VPC/subredes/security groups con acceso directo a RDS y endpoint S3

Después de subir el ZIP, ajusta **Runtime settings > Handler** al valor
indicado y configura VPC, subredes, security group y variables `DB_*`, igual
que en Init DB.

El Processor, Init DB y Dashboard comparten las variables `DB_TYPE`, `DB_HOST`,
`DB_PORT`, `DB_NAME`, `DB_USER` y `DB_PASSWORD`. El processor nunca usa SQLite
ni archivos locales cuando se invoca mediante su adaptador Lambda.

La guía completa se encuentra en
[`ARCHITECTURE.md`](../../ARCHITECTURE.md#despliegue-temporal-en-aws).
