from tests.functional.adapter._store_test_failures_base import StoreTestFailuresBase


class TestSparkStoreFailuresAll(StoreTestFailuresBase):
    def test_store_and_assert(self, project):
        self.run_tests_store_failures_and_assert(project)
