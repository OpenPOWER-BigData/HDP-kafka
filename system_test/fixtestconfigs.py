#!/usr/bin/python
import sys
import json
import os
import fnmatch
import collections
import fileinput
import re

# Settings loaded from cluster.properties file
brokers           = []
brokers_target    = []
zookeepers        = []
zookeepers_target = []
producers         = []
consumers         = []
consumers_target  = []
mirrormakers      = []

javaHome = ""
kafkaHome = ""
zkPort = ""
secure = False


def find_files(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result


def fix_cluster_config_files(directory):
    for f in find_files("cluster_config.json", directory):
        print "Processing " + f
        inFile = open(f, "r")
        data = json.load(inFile, object_pairs_hook=collections.OrderedDict)
        inFile.close()

        brokerIndx = 0
        t_brokerIndx = 0
        producerIndx = 0
        consumerIndx = 0
        t_consumerIndx = 0
        mirrormakerIndx = 0
        zkIndx = 0
        t_zkIndx = 0

        for entity in data["cluster_config"]:
            if entity["role"] == "broker" and len(brokers)>0:
                if "cluster_name" not in entity  or  entity["cluster_name"] == "source":
                    entity["hostname"] = brokers[brokerIndx]
                    brokerIndx = brokerIndx+1 if brokerIndx+1<len(brokers) else 0
                elif entity["cluster_name"] == "target":
                    entity["hostname"] = brokers_target[t_brokerIndx]
                    t_brokerIndx = t_brokerIndx+1 if t_brokerIndx+1<len(brokers_target) else 0
                else:
                    print "*** UNEXPECTED broker entity:  %s" % entity

            elif entity["role"] == "zookeeper" and len(zookeepers)>0:
                if "cluster_name" not in entity  or  entity["cluster_name"] == "source":
                    entity["hostname"] = zookeepers[zkIndx]
                    zkIndx = zkIndx+1 if zkIndx+1<len(zookeepers) else 0
                elif entity["cluster_name"] == "target":
                    entity["hostname"] = zookeepers_target[t_zkIndx]
                    t_zkIndx = t_zkIndx+1 if t_zkIndx+1<len(zookeepers_target) else 0
                else:
                    print "*** UNEXPECTED ZK entity:  %s" % entity
            elif entity["role"] == "producer_performance" and len(producers)>0:
                if "cluster_name" not in entity  or  entity["cluster_name"] == "source":
                    entity["hostname"] = producers[producerIndx]
                    producerIndx = producerIndx+1 if producerIndx+1<len(producers) else 0
                elif entity["cluster_name"] == "target":
                    print "*** UNEXPECTED Target Producer:  %s" % entity
            elif entity["role"] == "console_consumer" and len(consumers)>0:
                if "cluster_name" not in entity  or  entity["cluster_name"] == "source":
                    entity["hostname"] = consumers[consumerIndx]
                    consumerIndx = consumerIndx+1 if consumerIndx+1<len(consumers) else 0
                elif entity["cluster_name"] == "target":
                    entity["hostname"] = consumers_target[t_consumerIndx]
                    t_consumerIndx = t_consumerIndx+1 if t_consumerIndx+1<len(consumers_target) else 0
                else:
                    print "*** UNEXPECTED Consumer entity:  %s" % entity
            elif entity["role"] == "mirror_maker" and len(mirrormakers)>0:
                entity["hostname"] = mirrormakers[mirrormakerIndx]
                mirrormakerIndx = mirrormakerIndx+1 if mirrormakerIndx+1<len(mirrormakers) else 0

            if "java_home" in entity and javaHome!="":
                entity["java_home"] = javaHome
            if "kafka_home" in entity and kafkaHome!="":
                entity["kafka_home"] = kafkaHome

        outFile = open(f, "w+")
        outFile.write( json.dumps(data, indent=4, separators=(',', ': ')) )
        outFile.close()


def fix_json_properties_files(directory):
    for f in find_files("testcase_*_properties.json", directory):
        print "Processing " + f
        inFile = open(f, "r")
        data = json.load(inFile, object_pairs_hook=collections.OrderedDict)
        inFile.close()
        changed = False
        for entity in data["entities"]:
            if "zookeeper" in entity:
                entity["zookeeper"] = zookeepers[0] + ":" + zkPort
                changed = True

        if changed:
            outFile = open(f, "w+")
            outFile.write( json.dumps(data, indent=4, separators=(',', ': ')) )
            outFile.close()

def fix_other_properties_file(directory):
    if len(zookeepers) == 0:
        return


    for f in find_files("*.properties", directory):
        print "Processing " + f
        fname = os.path.basename(f)
        if fname == "mirror_consumer.properties":
            os.popen("perl -i -pe 's/zookeeper.connect=.*/zookeeper.connect=" + zookeepers_target[0] + "/' " + f).read()
        elif fname == "mirror_producer.properties":
            os.popen("perl -i -pe 's/metadata.broker.list=.*/metadata.broker.list=" + ",".join(brokers) + "/' " + f ).read()
            print os.popen("perl -i -pe 's/bootstrap.servers=.*/bootstrap.servers=" + ",".join(brokers) + "/' " + f ).read()
        else:
            os.popen("perl -i -pe 's/zookeeper.connect=localhost:.*/zookeeper.connect=" + zookeepers[0] + ":" + zkPort + "/' " + f).read()
            os.popen("perl -i -pe 's/zk.connect=localhost:.*/zk.connect=" + zookeepers[0] + ":" + zkPort + "/' " + f).read()

        if re.search("zookeeper.*properties", fname):
            # print os.popen("perl -i -pe 's/server.1=localhost/server.1=" + zookeepers[0] + "/' " + f).read()
            if secure:
                with open(f, "a") as zkConf:
                    zkConf.write("\nauthProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
                    zkConf.write("\njaasLoginRenew=3600000")
                    zkConf.write("\nkerberos.removeHostFromPrincipal=true")
                    zkConf.write("\nkerberos.removeRealmFromPrincipal=true\n")


        if secure and fname == "server.properties":
            with open(f, "a") as brokerconf:
                brokerconf.write("\nsuper.users=User:kafka")
                brokerconf.write("\nprincipal.to.local.class=kafka.security.auth.KerberosPrincipalToLocal")
                brokerconf.write("\nauthorizer.class.name=kafka.security.auth.SimpleAclAuthorizer")
                brokerconf.write("\nsecurity.inter.broker.protocol=SASL_PLAINTEXT\n")
        if secure and (fname == "producer.properties" or fname == "producer_performance.properties" or fname == "consumer.properties"):
            with open(f, "a") as producerconf:
                producerconf.write("\nsecurity.protocol=SASL_PLAINTEXT\n")


def loadClusterProperties(clusterProp):
    inFile = open(clusterProp, "r")
    data = json.load(inFile)
    inFile.close()
    global kafkaHome, javaHome, zkPort, zookeepers, producers, consumers, brokers, secure

    if not "zookeepers" in data:
        print >> sys.stderr, "'zookeepers' list not specified"
    else:
        for zk in data["zookeepers"]:
            zookeepers.append(zk)

    if not "zookeepers_target" in data:
        print >> sys.stderr, "'zookeepers_target' list not specified"
    else:
        for zk in data["zookeepers_target"]:
            zookeepers_target.append(zk)

    if not "brokers" in data:
        print >> sys.stderr, "'brokers' list not specified"
    else:
        for b in data["brokers"]:
            brokers.append(b)

    if not "brokers_target" in data:
        print >> sys.stderr, "'brokers_target' list not specified"
    else:
        for b in data["brokers_target"]:
            brokers_target.append(b)


    if not "producers" in data:
        print >> sys.stderr, "'producers' list not specified"
    else:
        for p in data["producers"]:
            producers.append(p)

    if not "mirrormakers" in data:
        print >> sys.stderr, "'mirrormakers' list not specified"
    else:
        for m in data["mirrormakers"]:
            mirrormakers.append(m)

    if not "zkPort" in data:
        print >> sys.stderr, "'zkPort' not specified"
    else:
        zkPort = data["zkPort"]

    if not "consumers" in data:
        print >> sys.stderr, "'consumers' list not specified"
    else:
        for c in data["consumers"]:
            consumers.append(c)

    if not "consumers_target" in data:
        print >> sys.stderr, "'consumers_target' list not specified"
    else:
        for c in data["consumers_target"]:
            consumers_target.append(c)


    if not "javaHome" in data:
        print >> sys.stderr, "'javaHome' not specified"
    else:
        javaHome = data["javaHome"]

    if not "kafkaHome" in data:
        print >> sys.stderr, "'kafaHome' not specified"
    else:
        kafkaHome = data["kafkaHome"]

    if not "secure" in data:
        secure = False
    else:
        secure = True if data['secure'] else False
    print "**** SECURE MODE = %s ****" % secure

# Main

def usage():
    print "Usage :"
    print sys.argv[0] + " cluster.json testsuite_dir/"

if not len(sys.argv) == 3:
    usage()
    exit(1)

clusterProp = sys.argv[1]
directory = sys.argv[2]  # "./system_test/offset_management_testsuite"

loadClusterProperties(clusterProp)

print "-Kafka Home: " + kafkaHome
print "-Java Home: " + javaHome
print "-ZK port : " + zkPort
print "-Consumers : " + ",".join( consumers )
print "-Consumers (Target): " + ",".join( consumers_target )
print "-Producers : " + ",".join( producers )
print "-Brokers : " + ",".join( brokers )
print "-Brokers (Target): " + ",".join( brokers_target )
print "-Mirror Makers : " + ",".join( mirrormakers )
print "-Zookeepers : " + ",".join( zookeepers )
print "-Zookeepers (Target): " + ",".join( zookeepers_target )
print "-Secure : %s " % secure

# 1 Update all cluster_config.json files
fix_cluster_config_files(directory)

# 2 Update testcase_*_properties.json files
fix_json_properties_files(directory)

# 3 fix role specific property files
fix_other_properties_file(directory)
