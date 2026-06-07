import zipfile

from tools import build_lambdas


def test_all_lambda_zips_include_declared_modules(tmp_path) -> None:
    for service, config in build_lambdas.SERVICES.items():
        output = build_lambdas.build(
            service,
            tmp_path,
            skip_dependencies=True,
            platform="manylinux2014_x86_64",
            python_version="3.12",
        )

        with zipfile.ZipFile(output) as archive:
            names = set(archive.namelist())

        expected = {
            f"crypto_arbitrage_aws/{module}"
            for module in config["modules"]
        }
        assert expected <= names
        assert not any("__pycache__" in name or name.endswith(".pyc") for name in names)
