# Despliegue de Lambdas

Construye ambos artefactos aislados para AWS Lambda Python 3.12 `x86_64`:

```bash
python tools/build_lambdas.py all
```

Para ARM64:

```bash
python tools/build_lambdas.py all --platform manylinux2014_aarch64
```

Los artefactos se escriben en `dist/lambdas/`.

## Poller

- ZIP: `poller.zip`
- Handler: `crypto_arbitrage_aws.lambdas.poller.lambda_handler`
- Variable obligatoria: `KINESIS_STREAM`
- IAM: `kinesis:PutRecords` sobre el stream de destino
- Trigger: EventBridge Scheduler

## Processor

- ZIP: `processor.zip`
- Handler: `crypto_arbitrage_aws.lambdas.processor.lambda_handler`
- Variables obligatorias: `DB_DSN`, `S3_BUCKET`
- Variable opcional: `MAX_PRICE_AGE_SECONDS` (default: `120`)
- Variable opcional: `ARBITRAGE_THRESHOLD_PCT` (default: `0.3`)
- IAM: lectura de Kinesis y `s3:PutObject` sobre `s3://<bucket>/raw_ticks/*`
- Trigger: Kinesis event source mapping
- Red: VPC/subredes/security groups con acceso directo a RDS y endpoint S3

Para la práctica temporal, usa directamente el endpoint de RDS en `DB_DSN`.
RDS Proxy queda como mejora para un despliegue de producción. El processor
nunca usa SQLite ni archivos locales cuando se invoca mediante su adaptador
Lambda.

La guía completa se encuentra en
[`ARCHITECTURE.md`](../../ARCHITECTURE.md#despliegue-temporal-en-aws).
