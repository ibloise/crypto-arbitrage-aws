#!/usr/bin/env python3
import argparse
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_PACKAGE = ROOT / "src" / "crypto_arbitrage_aws"

SERVICES = {
    "poller": {
        "requirements": ROOT / "deploy" / "lambdas" / "poller-requirements.txt",
        "modules": [
            "__init__.py",
            "contracts.py",
            "kinesis.py",
            "poller.py",
            "lambdas/__init__.py",
            "lambdas/config.py",
            "lambdas/poller.py",
        ],
    },
    "processor": {
        "requirements": ROOT / "deploy" / "lambdas" / "processor-requirements.txt",
        "modules": [
            "__init__.py",
            "contracts.py",
            "paths.py",
            "processor.py",
            "lambdas/__init__.py",
            "lambdas/config.py",
            "lambdas/processor.py",
        ],
    },
}


def copy_modules(staging: Path, modules: list[str]) -> None:
    destination = staging / "crypto_arbitrage_aws"
    for module in modules:
        source = SOURCE_PACKAGE / module
        target = destination / module
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def install_dependencies(
    staging: Path,
    requirements: Path,
    platform: str,
    python_version: str,
) -> None:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--requirement",
            str(requirements),
            "--target",
            str(staging),
            "--platform",
            platform,
            "--python-version",
            python_version,
            "--implementation",
            "cp",
            "--only-binary=:all:",
        ],
        check=True,
    )


def create_zip(staging: Path, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(staging.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(staging))


def build(
    service: str,
    output_dir: Path,
    skip_dependencies: bool,
    platform: str,
    python_version: str,
) -> Path:
    config = SERVICES[service]
    with tempfile.TemporaryDirectory(prefix=f"lambda-{service}-") as temporary:
        staging = Path(temporary)
        copy_modules(staging, config["modules"])
        if not skip_dependencies:
            install_dependencies(
                staging,
                config["requirements"],
                platform,
                python_version,
            )
        output = output_dir / f"{service}.zip"
        create_zip(staging, output)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Build isolated Lambda ZIP artifacts")
    parser.add_argument("service", choices=[*SERVICES, "all"])
    parser.add_argument("--output-dir", type=Path, default=ROOT / "dist" / "lambdas")
    parser.add_argument("--skip-dependencies", action="store_true")
    parser.add_argument("--platform", default="manylinux2014_x86_64")
    parser.add_argument("--python-version", default="3.12")
    args = parser.parse_args()

    services = SERVICES if args.service == "all" else [args.service]
    for service in services:
        print(
            build(
                service,
                args.output_dir,
                args.skip_dependencies,
                args.platform,
                args.python_version,
            )
        )


if __name__ == "__main__":
    main()
