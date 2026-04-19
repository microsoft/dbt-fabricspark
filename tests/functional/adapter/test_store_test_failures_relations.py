from tests.functional.adapter._store_test_failures_base import StoreTestFailuresBase


class TestSparkStoreFailuresRelations(StoreTestFailuresBase):
    def test_store_and_assert(self, project):
        self.run_tests_store_one_run_with_relations(project)
