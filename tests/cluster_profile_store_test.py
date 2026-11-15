"""cluster_profile_store 纯函数测试（不连集群/数据库）。"""
import unittest

from services.cluster_profile_store import (
    ProfileStoreError,
    bundle_hash,
    cluster_identity_hash,
    normalize_filename,
    validate_content,
)


class ClusterProfileStoreTest(unittest.TestCase):
    def test_normalize_filename(self):
        self.assertEqual(normalize_filename(r"C:\fakepath\core-site.xml"), "core-site.xml")
        self.assertEqual(normalize_filename("hive-site.xml"), "hive-site.xml")
        with self.assertRaises(ProfileStoreError):
            normalize_filename("evil.txt")
        with self.assertRaises(ProfileStoreError):
            normalize_filename("..")
        with self.assertRaises(ProfileStoreError):
            normalize_filename("")

    def test_xml_validation(self):
        valid = b"<configuration><property><name>a</name><value>1</value></property></configuration>"
        self.assertEqual(validate_content("core-site.xml", valid), "application/xml")
        with self.assertRaises(ProfileStoreError):
            validate_content("core-site.xml", b"<!DOCTYPE configuration [<!ENTITY x>]><configuration/>")
        with self.assertRaises(ProfileStoreError):
            validate_content("core-site.xml", b"<not-configuration/>")
        with self.assertRaises(ProfileStoreError):
            validate_content("core-site.xml", b"<configuration>")

    def test_yaml_and_properties(self):
        self.assertEqual(validate_content("flink-conf.yaml", b"jobmanager.rpc.address: node1\n"),
                         "application/yaml")
        with self.assertRaises(ProfileStoreError):
            validate_content("flink-conf.yaml", b"- a\n- b\n")
        self.assertEqual(validate_content("server.properties", b"bootstrap.servers=node1:9092\n"),
                         "text/plain")
        with self.assertRaises(ProfileStoreError):
            validate_content("server.properties", b"just-a-line\n")

    def test_empty_and_size_limit(self):
        with self.assertRaises(ProfileStoreError):
            validate_content("core-site.xml", b"")
        with self.assertRaises(ProfileStoreError) as ctx:
            validate_content("core-site.xml", b"x" * 10, max_file_bytes=5)
        self.assertEqual(ctx.exception.code, "too_large")

    def test_bundle_hash_order_independent(self):
        a = bundle_hash({"a.xml": "1" * 64, "b.yaml": "2" * 64})
        b = bundle_hash({"b.yaml": "2" * 64, "a.xml": "1" * 64})
        self.assertEqual(a, b)
        self.assertEqual(len(a), 64)

    def test_cluster_identity(self):
        x = cluster_identity_hash("hdfs://node1:8020", "thrift://node1:9083", "node1:9092")
        y = cluster_identity_hash("hdfs://node1:8020", "thrift://node1:9083", "node1:9092")
        z = cluster_identity_hash("hdfs://node2:8020", "thrift://node1:9083", "node1:9092")
        self.assertEqual(x, y)
        self.assertNotEqual(x, z)
        self.assertEqual(len(x), 64)
        self.assertEqual(
            cluster_identity_hash("", "", "", fallback_name="c1"),
            cluster_identity_hash("", "", "", fallback_name="c1"),
        )


if __name__ == "__main__":
    unittest.main()
