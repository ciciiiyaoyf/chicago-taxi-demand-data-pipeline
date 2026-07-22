"""Offline smoke tests for the recovered MGMT 405 pipeline repository."""

from __future__ import annotations

import ast
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
PYSPARK_FILE = ROOT / "dataproc" / "clean_and_aggregate_v3.py"
PIPELINE_FILE = ROOT / "pipeline.sh"
REFERENCE_SQL = ROOT / "snowflake" / "load_final_dataset.sql"

REQUIRED_ENV_VARS = (
    "GCP_PROJECT_ID",
    "GCS_BUCKET",
    "DATAPROC_CLUSTER",
    "DATAPROC_REGION",
    "PYSPARK_FILE",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_TABLE",
    "SNOWFLAKE_FILE_FORMAT",
    "SNOWFLAKE_STAGE",
)

REQUIRED_FILES = (
    "README.md",
    "pipeline.sh",
    "requirements.txt",
    ".gitignore",
    "dataproc/README.md",
    "dataproc/clean_and_aggregate_v3.py",
    "snowflake/README.md",
    "snowflake/load_final_dataset.sql",
    "docs/data_setup.md",
    "docs/pipeline_setup.md",
    "docs/snowflake_setup.md",
    "tableau/README.md",
    "tests/smoke_test.py",
    "archive/README.md",
)


def assignment_tuple(path: Path, name: str) -> tuple[str, ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    value = ast.literal_eval(node.value)
                    return tuple(value)
    raise AssertionError(f"Assignment {name} not found in {path}")


def sql_table_columns(path: Path) -> tuple[str, ...]:
    text = path.read_text(encoding="utf-8")
    try:
        table_tail = text.split("CREATE OR REPLACE TABLE", 1)[1]
        definition = table_tail.split(");", 1)[0]
    except IndexError as exc:
        raise AssertionError(f"Could not find CREATE TABLE block in {path}") from exc
    return tuple(
        match.group(1).lower()
        for match in re.finditer(r"(?m)^\s{2}([A-Z][A-Z0-9_]*)\s+", definition)
    )


def find_bash() -> str | None:
    located = shutil.which("bash")
    if located:
        return located
    if os.name == "nt":
        candidates = (
            Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Git/bin/bash.exe",
            Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Git/usr/bin/bash.exe",
            Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32/bash.exe",
        )
        for candidate in candidates:
            if candidate.is_file():
                return str(candidate)
    return None


class RepositorySmokeTests(unittest.TestCase):
    maxDiff = None

    def test_required_repository_files_exist(self) -> None:
        missing = [name for name in REQUIRED_FILES if not (ROOT / name).is_file()]
        self.assertEqual(missing, [], f"Missing repository files: {missing}")

    def test_production_python_has_valid_syntax(self) -> None:
        for relative in ("dataproc/clean_and_aggregate_v3.py", "tests/smoke_test.py"):
            path = ROOT / relative
            compile(path.read_text(encoding="utf-8"), str(path), "exec")

    def test_pipeline_has_valid_bash_syntax(self) -> None:
        bash = find_bash()
        self.assertIsNotNone(bash, "bash is required to run the requested bash -n check")
        result = subprocess.run(
            [bash, "-n", str(PIPELINE_FILE)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    def test_pipeline_uses_strict_mode_and_is_noninteractive(self) -> None:
        text = PIPELINE_FILE.read_text(encoding="utf-8")
        self.assertIn("set -euo pipefail", text)
        self.assertIsNone(re.search(r"(?m)^\s*read(?:\s|$)", text))
        self.assertNotIn("env_template.sh", text)

    def test_required_environment_names_appear_in_pipeline(self) -> None:
        text = PIPELINE_FILE.read_text(encoding="utf-8")
        missing = [name for name in REQUIRED_ENV_VARS if name not in text]
        self.assertEqual(missing, [], f"Variables absent from pipeline.sh: {missing}")
        for suffix in ("ACCOUNT", "USER", "PASSWORD"):
            obsolete = "SNOW_" + suffix
            self.assertNotRegex(text, rf"\b{obsolete}\b")

        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        undocumented = [name for name in REQUIRED_ENV_VARS if name not in readme]
        self.assertEqual(undocumented, [], f"Variables absent from README.md: {undocumented}")

    def test_pyspark_default_matches_real_file(self) -> None:
        text = PIPELINE_FILE.read_text(encoding="utf-8")
        match = re.search(
            r'PYSPARK_FILE="\$\{PYSPARK_FILE:-([^}]+)\}"',
            text,
        )
        self.assertIsNotNone(match, "PYSPARK_FILE default not found")
        self.assertEqual(match.group(1), "dataproc/clean_and_aggregate_v3.py")
        self.assertTrue((ROOT / match.group(1)).is_file())

    def test_sql_schemas_match_pyspark_output_contract(self) -> None:
        expected = assignment_tuple(PYSPARK_FILE, "FINAL_COLUMNS")
        self.assertEqual(len(expected), 23)
        self.assertEqual(sql_table_columns(REFERENCE_SQL), expected)
        self.assertEqual(sql_table_columns(PIPELINE_FILE), expected)

        sql = REFERENCE_SQL.read_text(encoding="utf-8")
        self.assertRegex(sql, r"TRACT_ID\s+VARCHAR\(11\)")
        self.assertIn("{{SNOWFLAKE_STAGE}}", sql)
        self.assertIn("{{SPARK_PART_FILE}}", sql)

    def test_no_obvious_credentials_or_account_specific_gcs_uris(self) -> None:
        text_files = []
        for path in ROOT.rglob("*"):
            if not path.is_file() or ".git" in path.parts or "__pycache__" in path.parts:
                continue
            try:
                text_files.append((path, path.read_text(encoding="utf-8")))
            except UnicodeDecodeError:
                continue

        password_assignment = re.compile(
            r"(?im)^\s*(?:export\s+)?[A-Z0-9_]*PASSWORD\s*=\s*(['\"])(?!YOUR_|\$|\{\{)[^'\"\r\n]+\1"
        )
        private_key = re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")
        real_gcs_uri = re.compile(r"gs://[a-z0-9][a-z0-9._-]{4,}")

        failures = []
        for path, text in text_files:
            if password_assignment.search(text):
                failures.append(f"hard-coded password assignment in {path.relative_to(ROOT)}")
            if private_key.search(text):
                failures.append(f"private key in {path.relative_to(ROOT)}")
            if real_gcs_uri.search(text):
                failures.append(f"account-specific GCS URI in {path.relative_to(ROOT)}")
        self.assertEqual(failures, [], "\n".join(failures))

    def test_readme_local_links_resolve(self) -> None:
        failures = []
        link_pattern = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
        for readme in ROOT.rglob("*.md"):
            for target in link_pattern.findall(readme.read_text(encoding="utf-8")):
                if target.startswith(("http://", "https://", "mailto:", "#")):
                    continue
                relative_target = target.split("#", 1)[0]
                if relative_target and not (readme.parent / relative_target).resolve().exists():
                    failures.append(f"{readme.relative_to(ROOT)} -> {target}")
        self.assertEqual(failures, [], f"Broken local links: {failures}")

    def test_pipeline_has_unix_line_endings(self) -> None:
        raw = PIPELINE_FILE.read_bytes()
        self.assertNotIn(b"\r\n", raw)
        self.assertTrue(raw.startswith(b"#!/usr/bin/env bash\n"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
