import unittest

from pydantic import ValidationError

from shared.contracts import TaskEnqueueItem, TaskEnqueueRequest


class SharedContractTests(unittest.TestCase):
    def test_enqueue_requires_tasks_and_caps_batch_size(self):
        with self.assertRaises(ValidationError):
            TaskEnqueueRequest(tasks=[])
        with self.assertRaises(ValidationError):
            TaskEnqueueRequest(
                tasks=[
                    TaskEnqueueItem(task_key=str(i), target_url="https://example.test")
                    for i in range(1001)
                ]
            )

    def test_legacy_order_task_metadata_maps_to_shared_contract(self):
        from master.enqueue_tasks import normalize_legacy_task

        task = normalize_legacy_task(
            {
                "hash_id": "stable-id",
                "type": "grid_url",
                "target_url": "https://maps.example.test/",
                "Search_Keyword": "coffee",
                "Search_Location": "Pune",
                "order_id": "order-1",
                "minimum_reviews": 5,
            }
        )
        self.assertEqual(task.task_key, "stable-id")
        self.assertEqual(task.task_type, "grid_url")
        self.assertEqual(task.search_keyword, "coffee")
        self.assertEqual(task.minimum_reviews, 5)


if __name__ == "__main__":
    unittest.main()
