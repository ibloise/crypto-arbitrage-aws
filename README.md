# Crypto Arbitrage AWS

Pipeline distribuido para capturar precios de criptomonedas, detectar
oportunidades de arbitraje y visualizar resultados. El proyecto puede
ejecutarse completamente en local o desplegarse temporalmente en AWS.

## Arquitectura

```text
Exchanges -> EC2 Collector -> Kinesis -> Lambda Processor -> RDS PostgreSQL
                                                |
                                                v
                                           S3 raw ticks

EC2 Dashboard ---------------------------------> RDS PostgreSQL
```

Los servicios se comunican mediante mensajes JSON homogéneos. El processor
mantiene los últimos precios por exchange, detecta spreads e implementa
idempotencia para soportar reintentos de Kinesis.

La explicación técnica, decisiones de diseño y guía completa de despliegue
están en [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Desarrollo local

Requiere Python 3.10 o superior:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[local,dev]"
```

Ejecutar el pipeline REST local:

```bash
crypto-arbitrage
```

Otros comandos disponibles:

```text
crypto-arbitrage-poller       Consulta precios REST
crypto-arbitrage-processor    Ejecuta una iteración completa
crypto-arbitrage-ws           Inicia el collector WebSocket
crypto-arbitrage-dashboard    Inicia el dashboard Streamlit
```

En local, el processor utiliza SQLite y guarda los ticks crudos bajo `data/`.
Los endpoints REST y WebSocket pueden sobrescribirse mediante variables de
entorno; si un endpoint REST falla, el Poller continúa con los proveedores
disponibles.

## Tests

```bash
python -m pytest
```

## Despliegue AWS

El despliegue académico recomendado utiliza:

- Una EC2 para el collector WebSocket.
- Kinesis Data Streams como bus de eventos.
- Lambda Init DB para inicializar el schema de PostgreSQL.
- Lambda Processor para detectar arbitraje.
- RDS PostgreSQL para resultados y últimos precios.
- S3 para ticks crudos.
- Una segunda EC2 para el dashboard Streamlit.

Consulta la sección [Despliegue temporal en AWS](docs/ARCHITECTURE.md#despliegue-temporal-en-aws)
para configuración, IAM, servicios `systemd` y orden de eliminación.

Los ZIP de Lambda se construyen con:

```bash
./scripts/build_lambdas.sh
```

Esto genera `init-db.zip`, `poller.zip` y `processor.zip`.

## Estructura

```text
src/crypto_arbitrage_aws/   Paquete y dominio
tests/                      Tests unitarios
deploy/lambdas/             Requisitos y notas de Lambda
tools/                      Herramientas de construcción
schema.sql                  Schema PostgreSQL
docs/ARCHITECTURE.md        Arquitectura y despliegue
```
