#!/usr/bin/env python3

import unittest

from benchmark_names import driver_name


class DriverNameTest(unittest.TestCase):
    def test_should_normalize_current_and_reverse_order_benchmark_names(self) -> None:
        # given
        current_this_driver = "example.PublicApiMatchedPoolThroughputBenchmark.thisDriver"
        current_client_v2 = "example.PublicApiMatchedPoolThroughputBenchmark.clientV2"
        reverse_order_this_driver = (
            "example.PublicApiMatchedPoolThroughputBenchmark.aThisDriver"
        )
        reverse_order_client_v2 = "example.PublicApiMatchedPoolThroughputBenchmark.zClientV2"

        # when
        normalized_names = (
            driver_name(current_this_driver),
            driver_name(current_client_v2),
            driver_name(reverse_order_this_driver),
            driver_name(reverse_order_client_v2),
        )

        # then
        self.assertEqual(
            normalized_names,
            ("thisDriver", "clientV2", "thisDriver", "clientV2"),
        )


if __name__ == "__main__":
    unittest.main()
