"""M9 file/AST and retained business-state baseline; no business writes."""

import ast
import hashlib
import json
from pathlib import Path

from sqlalchemy import text

from scripts.verify_milestone8 import source_integrity
from storage.postgres_redis_serving import get_postgres_engine


class WithoutInstrumentation(ast.NodeTransformer):
    def visit_ImportFrom(self, node):
        if node.module == "observability" and all(a.name == "telemetry" and a.asname == "m9" for a in node.names):
            return None
        return node

    @staticmethod
    def instrumentation(node):
        return isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and node.func.value.id == "m9"

    def visit_Expr(self, node):
        return None if self.instrumentation(node.value) else self.generic_visit(node)

    def visit_FunctionDef(self, node):
        node.decorator_list = [d for d in node.decorator_list if not self.instrumentation(d)]
        return self.generic_visit(node)

    def visit_If(self, node):
        node = self.generic_visit(node)
        return node if node.body or node.orelse else None


def fingerprint():
    root = Path(__file__).resolve().parents[1]
    result = {}
    for folder in ("producer", "kafka", "streaming", "data_quality", "storage", "dashboard", "alerts", "notifications", "scripts", "tests"):
        for path in (root / folder).rglob("*"):
            if path.suffix not in {".py", ".sql", ".md", ".json"} or "milestone9" in path.name or path.name == "test_observability.py":
                continue
            data = path.read_bytes()
            record = {"sha256": hashlib.sha256(data).hexdigest()}
            if path.suffix == ".py":
                tree = WithoutInstrumentation().visit(ast.parse(data))
                record["business_ast"] = hashlib.sha256(ast.dump(tree, include_attributes=False).encode()).hexdigest()
            result[str(path.relative_to(root))] = record
    return result


def state():
    engine = get_postgres_engine()
    try:
        result = {"m7": source_integrity(engine), "files": fingerprint()}
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn:
            with conn.begin():
                conn.execute(text("SET TRANSACTION READ ONLY"))
                for schema, tables in {"gold_serving": ("serving_state", "serving_history", "revenue_by_minute", "product_rankings"),
                                       "notifications_serving": ("source_binding", "notification_deliveries", "notification_attempts")}.items():
                    for table in tables:
                        rows = [dict(r) for r in conn.execute(text(f'SELECT * FROM "{schema}".{table} ORDER BY 1,2')).mappings()]
                        # Baseline/evidence only, never a scrape or recurring collector query.
                        encoded = json.dumps(rows, sort_keys=True, default=str, separators=(",", ":"))
                        result[schema + "." + table] = {"rows": len(rows), "sha256": hashlib.sha256(encoded.encode()).hexdigest()}
        return result
    finally:
        engine.dispose()


if __name__ == "__main__":
    print("MILESTONE9_VERIFIED " + json.dumps(state(), sort_keys=True))
