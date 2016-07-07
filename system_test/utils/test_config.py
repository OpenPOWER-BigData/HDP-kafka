from os import path
import json


class TestConfig(object):
    system_test_dir = path.dirname(path.dirname(path.realpath(__file__)))
    cluster_json_path = path.abspath(path.join(system_test_dir, "cluster.json"))
    with open(cluster_json_path) as cluster_json_file:
        cluster_dict = json.load(cluster_json_file)

    @classmethod
    def get_test_user(cls):
        return "hrt_qa"

    @classmethod
    def get_kinit_cmd(cls, secureMode):
        if not secureMode:
            return "echo;"
        kinit_cmd = cls.cluster_dict["kinit_cmd"] + ";"
        return kinit_cmd

