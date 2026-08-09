import logging
import shlex
import time

from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.product_catalog_missing_index_mitigation import (
    ProductCatalogMissingIndexMitigationOracle,
)
from sregym.conductor.problems.base import Problem
from sregym.service.apps.astronomy_shop import AstronomyShop
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

logger = logging.getLogger(__name__)


def _sql_failed(output: str) -> bool:
    upper = output.upper()
    return "ERROR:" in upper or "FATAL:" in upper


def _first_int(output: str) -> int:
    for line in reversed(output.strip().splitlines()):
        line = line.strip()
        if line:
            try:
                return int(line)
            except ValueError:
                return 0
    return 0


class ProductCatalogMissingIndexAstronomyShop(Problem):
    _POSTGRES_READY_TIMEOUT_SECONDS = 120
    _POSTGRES_POLL_INTERVAL_SECONDS = 3

    def __init__(self):
        self.app = AstronomyShop()
        self.namespace = self.app.namespace
        super().__init__(app=self.app, namespace=self.namespace)
        self.kubectl = KubeCtl()

        self.faulty_service = "product-catalog"
        self.backend_service = "postgresql"
        self.db_name = "otel"

        self.schema = "catalog"
        self.table = "products"
        self.id_column = "id"
        self._qualified = f"{self.schema}.{self.table}"

        self.seed_prefix = "PROD"
        self.target_rows = 3_000
        self.max_rows = 12_000
        self.batch_size = 5_000

        self.root_cause = self.build_structured_root_cause(
            component=f"deployment/{self.faulty_service}",
            namespace=self.namespace,
            description=(
                "The primary-key index on catalog.products(id) is missing, so "
                "product-catalog's GetProduct lookups "
                "(SELECT ... FROM catalog.products WHERE id = $1) perform "
                "sequential scans instead of index scans. All pods, including "
                "product-catalog and PostgreSQL, stay Running and healthy; the "
                "regression is only visible from the query plan (Seq Scan on "
                "catalog.products). The fix is to recreate the index on "
                "catalog.products(id)."
            ),
        )

        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)
        self.mitigation_oracle = ProductCatalogMissingIndexMitigationOracle(problem=self)
        self.app.create_workload()

    def _psql(self, sql: str, tuples_only: bool = False) -> str:
        psql_args = "-tAc" if tuples_only else "-c"
        script = (
            'PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" '
            f"-d {shlex.quote(self.db_name)} -v ON_ERROR_STOP=1 {psql_args} {shlex.quote(sql)}"
        )
        return self.kubectl.exec_command(
            f"kubectl exec -n {self.namespace} deploy/{self.backend_service} -- sh -lc {shlex.quote(script)}"
        )

    def _wait_postgres_ready(self) -> None:
        deadline = time.monotonic() + self._POSTGRES_READY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            out = self._psql("SELECT 1;", tuples_only=True)
            if _first_int(out) == 1:
                return
            time.sleep(self._POSTGRES_POLL_INTERVAL_SECONDS)
        raise RuntimeError("PostgreSQL did not become ready in time for fault injection.")

    def _analyze(self) -> None:
        self._psql(f"ANALYZE {self._qualified};")

    def _seed_row_count(self) -> int:
        out = self._psql(
            f"SELECT count(*) FROM {self._qualified} WHERE {self.id_column} LIKE '{self.seed_prefix}%';",
            tuples_only=True,
        )
        return _first_int(out)

    def _original_row_count(self) -> int:
        out = self._psql(
            f"SELECT count(*) FROM {self._qualified} WHERE {self.id_column} NOT LIKE '{self.seed_prefix}%';",
            tuples_only=True,
        )
        return _first_int(out)

    def _delete_seed_rows(self) -> None:
        self._psql(f"DELETE FROM {self._qualified} WHERE {self.id_column} LIKE '{self.seed_prefix}%';")

    def _seed_batch(self, start_g: int, end_g: int) -> None:
        sql = (
            f"INSERT INTO {self._qualified} "
            f"SELECT (jsonb_populate_record(NULL::{self._qualified}, "
            f"to_jsonb(t) || jsonb_build_object("
            f"'{self.id_column}', '{self.seed_prefix}' || lpad(g::text, 6, '0')))).* "
            f"FROM generate_series({start_g}, {end_g}) g "
            f"JOIN (SELECT p.*, row_number() OVER (ORDER BY {self.id_column}) - 1 AS _rn, "
            f"count(*) OVER () AS _n FROM {self._qualified} p "
            f"WHERE p.{self.id_column} NOT LIKE '{self.seed_prefix}%') t "
            f"ON t._rn = g % t._n;"
        )
        out = self._psql(sql)
        if _sql_failed(out):
            raise RuntimeError(f"Failed to seed catalog rows [{start_g}, {end_g}]: {out.strip()}")

    def _seed_to(self, target: int) -> None:
        current = self._seed_row_count()
        for start_g in range(current + 1, target + 1, self.batch_size):
            end_g = min(start_g + self.batch_size - 1, target)
            self._seed_batch(start_g, end_g)

    def _id_index_exists(self) -> bool:
        out = self._psql(
            "SELECT count(*) FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = i.indkey[0] "
            f"WHERE i.indisvalid AND n.nspname = '{self.schema}' AND c.relname = '{self.table}' "
            f"AND a.attname = '{self.id_column}';",
            tuples_only=True,
        )
        return _first_int(out) > 0

    def _pk_constraint_name(self) -> str | None:
        out = self._psql(
            f"SELECT conname FROM pg_constraint WHERE conrelid = '{self._qualified}'::regclass AND contype = 'p';",
            tuples_only=True,
        )
        if _sql_failed(out):
            raise RuntimeError(f"Could not read the primary key of {self._qualified}: {out.strip()}")
        return out.strip() or None

    def _drop_id_index(self) -> None:
        pk = self._pk_constraint_name()
        if pk:
            out = self._psql(f'ALTER TABLE {self._qualified} DROP CONSTRAINT "{pk}";')
            if _sql_failed(out):
                raise RuntimeError(f"Failed to drop primary key {pk!r}: {out.strip()}")
            return
        idx = self._psql(
            "SELECT indexrelid::regclass::text FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = i.indkey[0] "
            f"WHERE i.indisvalid AND n.nspname = '{self.schema}' AND c.relname = '{self.table}' "
            f"AND a.attname = '{self.id_column}' LIMIT 1;",
            tuples_only=True,
        ).strip()
        if idx and not _sql_failed(idx):
            self._psql(f"DROP INDEX {idx};")

    def _recreate_id_index(self) -> None:
        if self._id_index_exists():
            return
        out = self._psql(f"ALTER TABLE {self._qualified} ADD PRIMARY KEY ({self.id_column});")
        if _sql_failed(out):
            fallback = self._psql(
                f"CREATE INDEX IF NOT EXISTS {self.table}_{self.id_column}_idx "
                f"ON {self._qualified} ({self.id_column});"
            )
            if _sql_failed(fallback):
                raise RuntimeError(f"Failed to recreate id index: {out.strip()} / {fallback.strip()}")

    def explain_getproduct_plan(self) -> str:
        return self._psql(
            f"EXPLAIN SELECT * FROM {self._qualified} WHERE {self.id_column} = '{self.seed_prefix}000001';"
        )

    @staticmethod
    def plan_uses_index_scan(plan: str) -> bool:
        lowered = plan.lower()
        return ("index scan" in lowered or "index only scan" in lowered) and "seq scan" not in lowered

    def product_catalog_deployment_present(self) -> bool:
        try:
            deployment = self.kubectl.get_deployment(self.faulty_service, self.namespace)
        except Exception:
            return False
        replicas = deployment.spec.replicas
        return replicas is None or replicas > 0

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        self._wait_postgres_ready()

        self._delete_seed_rows()
        self._recreate_id_index()
        if self._original_row_count() < 1:
            raise RuntimeError(f"{self._qualified} has no template row to clone; cannot seed the catalog.")

        self._seed_to(self.target_rows)
        self._analyze()

        rows = self.target_rows
        while not self.plan_uses_index_scan(self.explain_getproduct_plan()) and rows < self.max_rows:
            rows = min(rows * 2, self.max_rows)
            logger.info("[missing-index] Planner not yet index-preferring; growing catalog to %d rows", rows)
            self._seed_to(rows)
            self._analyze()
        if not self.plan_uses_index_scan(self.explain_getproduct_plan()):
            raise RuntimeError(
                f"Planner did not choose an Index Scan even at {rows} rows; cannot establish a "
                "reliable index-vs-seqscan signal on this machine."
            )

        self._drop_id_index()
        self._analyze()

        plan = self.explain_getproduct_plan()
        if "seq scan" not in plan.lower():
            raise RuntimeError(f"Expected a Seq Scan after dropping the index, but the plan was:\n{plan}")

        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        self._wait_postgres_ready()
        self._delete_seed_rows()
        if not self._id_index_exists():
            self._recreate_id_index()

        self._analyze()
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")
