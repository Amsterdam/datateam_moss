# Databricks notebook source
import unittest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from reversed_modeling import Dataset

class TestDataset(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.dataset = Dataset(self.spark, 'catalog', 'schema', 'dataset')

    def test_get_information_schema(self):
        self.dataset.get_information_schema = MagicMock()
        self.dataset.get_information_schema.return_value = MagicMock()
        self.assertIsNotNone(self.dataset.get_information_schema())

if __name__ == '__main__':
    unittest.main()