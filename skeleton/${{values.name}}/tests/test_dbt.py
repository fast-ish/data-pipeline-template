import subprocess


def test_dbt_compile():
    result = subprocess.run(
        ["dbt", "compile", "--project-dir", "dbt"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"dbt compile failed: {result.stderr}"


def test_dbt_parse():
    result = subprocess.run(
        ["dbt", "parse", "--project-dir", "dbt"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"dbt parse failed: {result.stderr}"
