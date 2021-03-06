# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from ducktape.services.service import Service

from kafkatest.services.kafka.directory import kafka_dir

import subprocess
import time


class ZookeeperService(Service):

    logs = {
        "zk_log": {
            "path": "/mnt/zk.log",
            "collect_default": True},
        "zk_data": {
            "path": "/mnt/zookeeper",
            "collect_default": False}
    }

    def __init__(self, context, num_nodes):
        """
        :type context
        """
        super(ZookeeperService, self).__init__(context, num_nodes)

    def start_node(self, node):
        idx = self.idx(node)
        self.logger.info("Starting ZK node %d on %s", idx, node.account.hostname)

        node.account.ssh("mkdir -p /mnt/zookeeper")
        node.account.ssh("echo %d > /mnt/zookeeper/myid" % idx)

        config_file = self.render('zookeeper.properties')
        self.logger.info("zookeeper.properties:")
        self.logger.info(config_file)
        node.account.create_file("/mnt/zookeeper.properties", config_file)

        start_cmd = "/opt/%s/bin/zookeeper-server-start.sh " % kafka_dir(node)
        start_cmd += "/mnt/zookeeper.properties 1>> %(path)s 2>> %(path)s &" % self.logs["zk_log"]
        node.account.ssh(start_cmd)

        time.sleep(5)  # give it some time to start

    def pids(self, node):
        try:
            cmd = "ps ax | grep -i zookeeper | grep java | grep -v grep | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (subprocess.CalledProcessError, ValueError) as e:
            return []

    def alive(self, node):
        return len(self.pids(node)) > 0

    def stop_node(self, node):
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        node.account.kill_process("zookeeper", allow_fail=False)

    def clean_node(self, node):
        self.logger.info("Cleaning ZK node %d on %s", self.idx(node), node.account.hostname)
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        node.account.kill_process("zookeeper", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf /mnt/zookeeper /mnt/zookeeper.properties /mnt/zk.log", allow_fail=False)

    def connect_setting(self):
        return ','.join([node.account.hostname + ':2181' for node in self.nodes])
