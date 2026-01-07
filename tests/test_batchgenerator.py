import unittest
import sys
from pathlib import Path


# ============================================================
# AJOUT DU CHEMIN RACINE DU PROJET
# ============================================================
# Ce script est dans /test/
# Donc on remonte d'un cran pour aller à la racine du projet
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# ============================================================
# IMPORT ABSOLU PROPRE
# ============================================================
# Maintenant Python voit le dossier racine, donc ceci fonctionne :
from DockerETL_Images.Ingestion.IMDBCurler.scripts.utils.batch_generator import (
    BatchGenerator,
)


class TestBatchGenerator(unittest.TestCase):
    """Test cases for the BatchGenerator class"""

    def test_basic_batching(self):
        """Test that items are correctly batched into groups of batch_size"""
        data = list(range(10))
        generator = iter(data)
        batch_gen = BatchGenerator(generator, batch_size=3)

        batches = list(batch_gen)

        self.assertEqual(len(batches), 4)
        self.assertEqual(batches[0], [0, 1, 2])
        self.assertEqual(batches[1], [3, 4, 5])
        self.assertEqual(batches[2], [6, 7, 8])
        self.assertEqual(batches[3], [9])

    def test_exact_batch_size(self):
        """Test when data size is exactly divisible by batch_size"""
        data = list(range(9))
        generator = iter(data)
        batch_gen = BatchGenerator(generator, batch_size=3)

        batches = list(batch_gen)

        self.assertEqual(len(batches), 3)
        self.assertEqual(batches[0], [0, 1, 2])
        self.assertEqual(batches[1], [3, 4, 5])
        self.assertEqual(batches[2], [6, 7, 8])

    def test_single_batch(self):
        """Test when all data fits in a single batch"""
        data = [1, 2, 3]
        generator = iter(data)
        batch_gen = BatchGenerator(generator, batch_size=10)

        batches = list(batch_gen)

        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0], [1, 2, 3])

    def test_empty_generator(self):
        """Test with an empty generator"""
        data = []
        generator = iter(data)
        batch_gen = BatchGenerator(generator, batch_size=3)

        batches = list(batch_gen)

        self.assertEqual(len(batches), 0)

    def test_filter_func_removes_items(self):
        """Test that filter_func correctly removes items from batches"""
        data = list(range(10))
        generator = iter(data)

        def filter_even(item):
            return item % 2 == 0

        batch_gen = BatchGenerator(generator, batch_size=3, filter_func=filter_even)
        batches = list(batch_gen)

        self.assertEqual(len(batches), 2)
        self.assertEqual(batches[0], [1, 3, 5])
        self.assertEqual(batches[1], [7, 9])

    def test_filter_func_filters_all(self):
        """Test when filter_func filters out all items"""
        data = [2, 4, 6, 8]
        generator = iter(data)

        def filter_all(item):
            return True

        batch_gen = BatchGenerator(generator, batch_size=2, filter_func=filter_all)
        batches = list(batch_gen)

        self.assertEqual(len(batches), 0)

    def test_filter_func_keeps_all(self):
        """Test when filter_func keeps all items"""
        data = list(range(5))
        generator = iter(data)

        def keep_all(item):
            return False

        batch_gen = BatchGenerator(generator, batch_size=2, filter_func=keep_all)
        batches = list(batch_gen)

        self.assertEqual(len(batches), 3)
        self.assertEqual(batches[0], [0, 1])
        self.assertEqual(batches[1], [2, 3])
        self.assertEqual(batches[2], [4])

    def test_batch_size_one(self):
        """Test with batch_size of 1"""
        data = [1, 2, 3]
        generator = iter(data)
        batch_gen = BatchGenerator(generator, batch_size=1)

        batches = list(batch_gen)

        self.assertEqual(len(batches), 3)
        self.assertEqual(batches[0], [1])
        self.assertEqual(batches[1], [2])
        self.assertEqual(batches[2], [3])

    def test_with_dict_data(self):
        """Test BatchGenerator with dictionary items"""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
            {"id": 4, "name": "David"},
        ]
        generator = iter(data)
        batch_gen = BatchGenerator(generator, batch_size=2)

        batches = list(batch_gen)

        self.assertEqual(len(batches), 2)
        self.assertEqual(len(batches[0]), 2)
        self.assertEqual(batches[0][0]["name"], "Alice")
        self.assertEqual(batches[1][1]["name"], "David")

    def test_filter_with_dict_data(self):
        """Test filter_func with dictionary items"""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
            {"id": 4, "name": "David"},
        ]
        generator = iter(data)

        # Filter to skip items with even ID
        def filter_even_id(item):
            return item["id"] % 2 == 0

        batch_gen = BatchGenerator(generator, batch_size=2, filter_func=filter_even_id)
        batches = list(batch_gen)

        # Should keep only odd IDs: [1,3]
        self.assertEqual(len(batches), 1)
        self.assertEqual(len(batches[0]), 2)
        self.assertEqual(batches[0][0]["id"], 1)
        self.assertEqual(batches[0][1]["id"], 3)

    def test_iterator_protocol(self):
        """Test that BatchGenerator implements iterator protocol correctly"""
        data = [1, 2, 3, 4, 5]
        generator = iter(data)
        batch_gen = BatchGenerator(generator, batch_size=2)

        # Test __iter__ returns self
        self.assertIs(iter(batch_gen), batch_gen)

        # Test __next__ works
        batch1 = next(batch_gen)
        self.assertEqual(batch1, [1, 2])

        batch2 = next(batch_gen)
        self.assertEqual(batch2, [3, 4])

        batch3 = next(batch_gen)
        self.assertEqual(batch3, [5])

        # Should raise StopIteration when exhausted
        with self.assertRaises(StopIteration):
            next(batch_gen)

    def test_large_batch_size(self):
        """Test with batch_size larger than data"""
        data = list(range(5))
        generator = iter(data)
        batch_gen = BatchGenerator(generator, batch_size=100)

        batches = list(batch_gen)

        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0], [0, 1, 2, 3, 4])

    def test_completed_flag(self):
        """Test that completed flag prevents further iteration"""
        data = [1, 2, 3]
        generator = iter(data)
        batch_gen = BatchGenerator(generator, batch_size=5)

        # Exhaust the generator
        list(batch_gen)

        # Trying to iterate again should raise StopIteration immediately
        with self.assertRaises(StopIteration):
            next(batch_gen)

    def test_filter_func_with_strings(self):
        """Test filter_func with string data"""
        data = ["apple", "banana", "apricot", "cherry", "avocado"]
        generator = iter(data)

        # Filter to skip words starting with 'a'
        def filter_starts_with_a(item):
            return item.startswith("a")

        batch_gen = BatchGenerator(
            generator, batch_size=2, filter_func=filter_starts_with_a
        )
        batches = list(batch_gen)

        # Should keep only non-'a' words: [banana, cherry]
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0], ["banana", "cherry"])


class TestBatchGeneratorAdvanced(unittest.TestCase):
    """Advanced test cases for edge cases and complex scenarios"""

    def test_large_dataset_performance(self):
        """Test batching with a large dataset"""
        data = list(range(10000))
        generator = iter(data)
        batch_gen = BatchGenerator(generator, batch_size=100)

        batches = list(batch_gen)

        # Should have 100 batches
        self.assertEqual(len(batches), 100)
        # Each batch should have 100 items
        for batch in batches:
            self.assertEqual(len(batch), 100)

    def test_generator_generator(self):
        """Test with a generator function"""

        def gen_func():
            for i in range(8):
                yield i

        batch_gen = BatchGenerator(gen_func(), batch_size=3)
        batches = list(batch_gen)

        self.assertEqual(len(batches), 3)
        self.assertEqual(batches[0], [0, 1, 2])
        self.assertEqual(batches[1], [3, 4, 5])
        self.assertEqual(batches[2], [6, 7])

    def test_filter_modifies_items(self):
        """Test that filter function that modifies items works correctly"""
        data = [
            {"value": 1, "keep": True},
            {"value": 2, "keep": False},
            {"value": 3, "keep": True},
            {"value": 4, "keep": False},
        ]
        generator = iter(data)

        def filter_func(row):
            # Remove 'keep' field from all items
            if "keep" in row:
                del row["keep"]
            # Return True (skip) if was marked as False
            return False

        batch_gen = BatchGenerator(generator, batch_size=2, filter_func=filter_func)
        batches = list(batch_gen)

        # All items should have 'keep' removed
        for batch in batches:
            for item in batch:
                self.assertNotIn("keep", item)

    def test_multiple_filter_passes(self):
        """Test that each batch is properly filtered"""
        data = list(range(20))
        generator = iter(data)

        # Filter odd numbers
        def filter_odd(item):
            return item % 2 == 1

        batch_gen = BatchGenerator(generator, batch_size=3, filter_func=filter_odd)
        batches = list(batch_gen)

        # Should have 4 batches of even numbers
        expected = [[0, 2, 4], [6, 8, 10], [12, 14, 16], [18]]
        self.assertEqual(batches, expected)


if __name__ == "__main__":
    unittest.main()
