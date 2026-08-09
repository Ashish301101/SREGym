from sregym.conductor.oracles.base import Oracle


class ProductCatalogMissingIndexMitigationOracle(Oracle):
    importance = 1.0

    def evaluate(self, *args, **kwargs) -> dict:
        print("== Mitigation Evaluation ==")
        results = {
            "success": False,
            "deployment_present": False,
            "uses_index_scan": False,
            "plan": None,
            "reason": "",
        }
        problem = self.problem

        try:
            results["deployment_present"] = problem.product_catalog_deployment_present()
        except Exception as exc:
            results["reason"] = f"could not read the product-catalog deployment: {exc}"
            return results
        if not results["deployment_present"]:
            results["reason"] = "product-catalog deployment is missing or scaled to zero"
            return results

        try:
            plan = problem.explain_getproduct_plan()
        except Exception as exc:
            results["reason"] = f"could not EXPLAIN the GetProduct query: {exc}"
            return results

        results["plan"] = plan.strip()
        results["uses_index_scan"] = problem.plan_uses_index_scan(plan)
        if not results["uses_index_scan"]:
            results["reason"] = "GetProduct still performs a Seq Scan on catalog.products; index not restored"
            print("Mitigation Result: Fail")
            return results

        results["success"] = True
        results["reason"] = "GetProduct uses an Index Scan on catalog.products; index restored"
        print("Mitigation Result: Pass")
        return results
