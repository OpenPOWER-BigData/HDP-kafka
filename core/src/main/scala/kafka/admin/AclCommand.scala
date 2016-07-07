/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import java.util.Properties

import joptsimple._
import kafka.security.auth._
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._

object AclCommand {

  val Newline = scala.util.Properties.lineSeparator
  val ResourceTypeToValidOperations = Map[ResourceType, Set[Operation]] (
    Topic -> Set(Read, Write, Describe, All),
    Group -> Set(Read, All),
    Cluster -> Set(Create, ClusterAction, All)
  )

  def main(args: Array[String]) {

    val opts = new AclCommandOptions(args)

    if (opts.options.has(opts.helpOpt))
      CommandLineUtils.printUsageAndDie(opts.parser, "Usage:")

    opts.checkArgs()

    var authorizerProperties = Map.empty[String, Any]
    if (opts.options.has(opts.authorizerPropertiesOpt)) {
      val props = opts.options.valuesOf(opts.authorizerPropertiesOpt).asScala.map(_.split("="))
      props.foreach(pair => authorizerProperties += (pair(0).trim -> pair(1).trim))
    }

    if(opts.options.has(opts.upgradeAclsOpt)) {
      upgradeAclsToNewFormat(authorizerProperties)
    }

    val authorizerClass = opts.options.valueOf(opts.authorizerOpt)
    val authZ: Authorizer = CoreUtils.createObject(authorizerClass)
    authZ.configure(authorizerProperties.asJava)

    if(opts.options.has(opts.downgradeAclsOpt)) {
      downgradeAclsToOldFormat(authorizerProperties, authZ)
    }

    try {
      if (opts.options.has(opts.addOpt))
        addAcl(opts)
      else if (opts.options.has(opts.removeOpt))
        removeAcl(opts)
      else if (opts.options.has(opts.listOpt))
        listAcl(opts)
    } catch {
      case e: Throwable =>
        println(s"Error while executing ACL command: ${e.getMessage}")
        println(Utils.stackTrace(e))
        System.exit(-1)
    }
  }

  def withAuthorizer(opts: AclCommandOptions)(f: Authorizer => Unit) {
    val authorizerProperties =
      if (opts.options.has(opts.authorizerPropertiesOpt)) {
        val authorizerProperties = opts.options.valuesOf(opts.authorizerPropertiesOpt).asScala
        CommandLineUtils.parseKeyValueArgs(authorizerProperties, acceptMissingValue = false).asScala
      } else {
        Map.empty[String, Any]
      }

    val authorizerClass = opts.options.valueOf(opts.authorizerOpt)
    val authZ = CoreUtils.createObject[Authorizer](authorizerClass)
    authZ.configure(authorizerProperties.asJava)
    try f(authZ)
    finally CoreUtils.swallow(authZ.close())
  }

  private def addAcl(opts: AclCommandOptions) {
    withAuthorizer(opts) { authorizer =>
      val resourceToAcl = getResourceToAcls(opts)

      if (resourceToAcl.values.exists(_.isEmpty))
        CommandLineUtils.printUsageAndDie(opts.parser, "You must specify one of: --allow-principal, --deny-principal when trying to add acls.")

      for ((resource, acls) <- resourceToAcl) {
        val acls = resourceToAcl(resource)
        println(s"Adding following acls for resource: $resource $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline")
        authorizer.addAcls(acls, resource)
      }

      listAcl(opts)
    }
  }

  private def removeAcl(opts: AclCommandOptions) {
    withAuthorizer(opts) { authorizer =>
      val resourceToAcl = getResourceToAcls(opts)

      for ((resource, acls) <- resourceToAcl) {
        if (acls.isEmpty) {
          if (confirmAction(s"Are you sure you want to delete all acls for resource: $resource y/n?"))
            authorizer.removeAcls(resource)
        } else {
          if (confirmAction(s"Are you sure you want to remove acls: $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline from resource $resource y/n?"))
            authorizer.removeAcls(acls, resource)
        }
      }

      listAcl(opts)
    }
  }

  private def listAcl(opts: AclCommandOptions) {
    withAuthorizer(opts) { authorizer =>
      val resources = getResource(opts, dieIfNoResourceFound = false)

      val resourceToAcls = if (resources.isEmpty)
        authorizer.getAcls()
      else
        resources.map(resource => (resource -> authorizer.getAcls(resource)))

      for ((resource, acls) <- resourceToAcls)
        println(s"Following is list of acls for resource: $resource $Newline ${acls.map("\t" + _).mkString(Newline)} $Newline")
    }
  }

  private def upgradeAclsToNewFormat(configs: Map[String, Any]) {
    val zkUtils: ZkUtils = getZkUtil(configs)

    val newResourceTypeNames = ResourceType.values.map(_.name)
    val oldResourceTypes = zkUtils.getChildrenParentMayNotExist(SimpleAclAuthorizer.AclZkPath).filter(resourceType => !newResourceTypeNames.contains(resourceType))
    for (oldResourceType <- oldResourceTypes) {
      val resourceNames = zkUtils.getChildrenParentMayNotExist(s"${SimpleAclAuthorizer.AclZkPath}/$oldResourceType")
      val resourceType = getUpgradeResourceType(oldResourceType)
      for(resourceName <- resourceNames) {
        var acls = Set.empty[Acl]
        println(s"migration of acls for $oldResourceType-$resourceName is in progress")
        val aclJson = zkUtils.readData(s"${SimpleAclAuthorizer.AclZkPath}/$oldResourceType/$resourceName")._1
        val aclList = Json.parseFull(aclJson).get.asInstanceOf[Map[String, Any]](Acl.AclsKey).asInstanceOf[List[Map[String, Any]]]
        for (aclMap <- aclList) {
          val hosts = aclMap("hosts").asInstanceOf[List[String]]
          val operations = aclMap("operations").asInstanceOf[List[String]]
          val principals = aclMap("principals").asInstanceOf[List[String]]
          val permissionType = aclMap("permissionType").toString
          for (operation <- operations) {
            for (host <- hosts) {
              for (principal <- principals)
                acls = acls + new Acl(getUpgradePrincipal(principal), getUpgradePermissionType(permissionType), host, getUpgradeOpertion(operation))
            }
          }
        }

        zkUtils.createPersistentPath(s"${SimpleAclAuthorizer.AclZkPath}/$resourceType/$resourceName", Json.encode(Acl.toJsonCompatibleMap(acls)))
        println(s"migrated acls from $oldResourceType-$resourceName to $resourceType-$resourceName")
      }
    }
    println("Done Migrating all old acls to new acls, will now attempt to delete the old acls.")

    for (invalidResourceType <- oldResourceTypes) {
      zkUtils.deletePathRecursive(s"${SimpleAclAuthorizer.AclZkPath}/$invalidResourceType")
    }

    println("All old acls are now deleted and migrated to new version of acl.")
    System.exit(0)
  }


  private def downgradeAclsToOldFormat(configs: Map[String, Any], authorizer: Authorizer) {
    val zkUtils: ZkUtils = getZkUtil(configs)

    val resourceToAcls = authorizer.getAcls()
    for((resource, acls) <- resourceToAcls) {
      val resourceName = resource.name
      val resourceType = getDowngradeResourceType(resource.resourceType)
      var downgradeAcls = Set.empty[Map[String, Any]]
      println(s"migration of acls for ${resource.resourceType}-$resourceName is in progress")
      for(acl <- acls) {
        val host = acl.host
        val principal = acl.principal.toString
        val permissionType = getDowngradePermissionType(acl.permissionType)
        val operation = getDowngradeOpertion(acl.operation)
        downgradeAcls += Map("hosts" -> List(host), "principals" -> List(principal), "operations" -> List(operation), "permissionType" -> permissionType)
      }

      val json = Json.encode(Map(Acl.VersionKey -> Acl.CurrentVersion, Acl.AclsKey -> downgradeAcls))
      zkUtils.createPersistentPath(s"${SimpleAclAuthorizer.AclZkPath}/$resourceType/$resourceName", json)
      println(s"migrated acls from ${resource.resourceType}-$resourceName to $resourceType-$resourceName")
    }

    println("Done Migrating all new acls to old acls, will now attempt to delete the new acls.")
    val validResourceTypeNames = ResourceType.values.map(_.name)
    for(resourceType <- validResourceTypeNames) {
      zkUtils.deletePathRecursive(s"${SimpleAclAuthorizer.AclZkPath}/$resourceType")
    }
    println("All new acls are now deleted and migrated to old version of acl.")
    System.exit(0)
  }

  def getZkUtil(configs: Map[String, Any]): ZkUtils = {
    val props = new Properties()
    configs foreach { case (key, value) => props.put(key, value.toString) }
    val kafkaConfig = KafkaConfig.fromProps(props)
    val zkUrl = configs.getOrElse(SimpleAclAuthorizer.ZkUrlProp, kafkaConfig.zkConnect).toString
    val zkConnectionTimeoutMs = configs.getOrElse(SimpleAclAuthorizer.ZkConnectionTimeOutProp, kafkaConfig.zkConnectionTimeoutMs).toString.toInt
    val zkSessionTimeOutMs = configs.getOrElse(SimpleAclAuthorizer.ZkSessionTimeOutProp, kafkaConfig.zkSessionTimeoutMs).toString.toInt

    val zkUtils = ZkUtils(zkUrl, zkConnectionTimeoutMs, zkSessionTimeOutMs, JaasUtils.isZkSecurityEnabled())
    zkUtils
  }

  private def getUpgradeResourceType(resourceType: String): ResourceType  = {
    resourceType match {
      case "TOPIC" => Topic
      case "CONSUMER_GROUP" => Group
      case "CLUSTER" => Cluster
    }
  }

  private def getUpgradePermissionType(permissionType: String): PermissionType = {
    permissionType.toUpperCase match {
      case "ALLOW" => Allow
      case "DENY" => Deny
    }
  }

  private def getUpgradeOpertion(operation: String): Operation = {
    operation.toUpperCase match {
      case "READ" => Read
      case "WRITE" => Write
      case "CLUSTER_ACTION" => ClusterAction
      case "CREATE" => Create
      case "DESCRIBE" => Describe
      case "DELETE" => Delete
      case "ALTER" => Alter
      case "ALL" => All
    }
  }

  private def getUpgradePrincipal(principal: String): KafkaPrincipal  = {
    new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principal.split(KafkaPrincipal.SEPARATOR)(1))
  }

  private def getDowngradeResourceType(resourceType: ResourceType): String  = {
    resourceType match {
      case Topic => "TOPIC"
      case Group => "CONSUMER_GROUP"
      case Cluster => "CLUSTER"
    }
  }

  private def getDowngradePermissionType(permissionType: PermissionType): String = {
    permissionType match {
      case Allow => "ALLOW"
      case Deny => "DENY"
    }
  }

  private def getDowngradeOpertion(operation: Operation): String = {
    operation match {
      case Read => "READ"
      case Write => "WRITE"
      case ClusterAction => "CLUSTER_ACTION"
      case Create => "CREATE"
      case Describe => "DESCRIBE"
      case Delete => "DELETE"
      case Alter  => "ALTER"
      case All => "ALL"
    }
  }

  private def getResourceToAcls(opts: AclCommandOptions): Map[Resource, Set[Acl]] = {
    var resourceToAcls = Map.empty[Resource, Set[Acl]]

    //if none of the --producer or --consumer options are specified , just construct acls from CLI options.
    if (!opts.options.has(opts.producerOpt) && !opts.options.has(opts.consumerOpt)) {
      resourceToAcls ++= getCliResourceToAcls(opts)
    }

    //users are allowed to specify both --producer and --consumer options in a single command.
    if (opts.options.has(opts.producerOpt))
      resourceToAcls ++= getProducerResourceToAcls(opts)

    if (opts.options.has(opts.consumerOpt))
      resourceToAcls ++= getConsumerResourceToAcls(opts).map { case (k, v) => k -> (v ++ resourceToAcls.getOrElse(k, Set.empty[Acl])) }

    validateOperation(opts, resourceToAcls)

    resourceToAcls
  }

  private def getProducerResourceToAcls(opts: AclCommandOptions): Map[Resource, Set[Acl]] = {
    val topics: Set[Resource] = getResource(opts).filter(_.resourceType == Topic)

    val acls = getAcl(opts, Set(Write, Describe))

    //Write, Describe permission on topics, Create permission on cluster
    topics.map(_ -> acls).toMap[Resource, Set[Acl]] +
      (Resource.ClusterResource -> getAcl(opts, Set(Create)))
  }

  private def getConsumerResourceToAcls(opts: AclCommandOptions): Map[Resource, Set[Acl]] = {
    val resources = getResource(opts)

    val topics: Set[Resource] = getResource(opts).filter(_.resourceType == Topic)
    val groups: Set[Resource] = resources.filter(_.resourceType == Group)

    //Read,Describe on topic, Read on consumerGroup + Create on cluster

    val acls = getAcl(opts, Set(Read, Describe))

    topics.map(_ -> acls).toMap[Resource, Set[Acl]] ++
      groups.map(_ -> getAcl(opts, Set(Read))).toMap[Resource, Set[Acl]]
  }

  private def getCliResourceToAcls(opts: AclCommandOptions): Map[Resource, Set[Acl]] = {
    val acls = getAcl(opts)
    val resources = getResource(opts)
    resources.map(_ -> acls).toMap
  }

  private def getAcl(opts: AclCommandOptions, operations: Set[Operation]): Set[Acl] = {
    val allowedPrincipals = getPrincipals(opts, opts.allowPrincipalsOpt)

    val deniedPrincipals = getPrincipals(opts, opts.denyPrincipalsOpt)

    val allowedHosts = getHosts(opts, opts.allowHostsOpt, opts.allowPrincipalsOpt)

    val deniedHosts = getHosts(opts, opts.denyHostssOpt, opts.denyPrincipalsOpt)

    val acls = new collection.mutable.HashSet[Acl]
    if (allowedHosts.nonEmpty && allowedPrincipals.nonEmpty)
      acls ++= getAcls(allowedPrincipals, Allow, operations, allowedHosts)

    if (deniedHosts.nonEmpty && deniedPrincipals.nonEmpty)
      acls ++= getAcls(deniedPrincipals, Deny, operations, deniedHosts)

    acls.toSet
  }

  private def getAcl(opts: AclCommandOptions): Set[Acl] = {
    val operations = opts.options.valuesOf(opts.operationsOpt).asScala.map(operation => Operation.fromString(operation.trim)).toSet
    getAcl(opts, operations)
  }

  def getAcls(principals: Set[KafkaPrincipal], permissionType: PermissionType, operations: Set[Operation],
              hosts: Set[String]): Set[Acl] = {
    for {
      principal <- principals
      operation <- operations
      host <- hosts
    } yield new Acl(principal, permissionType, host, operation)
  }

  private def getHosts(opts: AclCommandOptions, hostOptionSpec: ArgumentAcceptingOptionSpec[String],
                       principalOptionSpec: ArgumentAcceptingOptionSpec[String]): Set[String] = {
    if (opts.options.has(hostOptionSpec))
      opts.options.valuesOf(hostOptionSpec).asScala.map(_.trim).toSet
    else if (opts.options.has(principalOptionSpec))
      Set[String](Acl.WildCardHost)
    else
      Set.empty[String]
  }

  private def getPrincipals(opts: AclCommandOptions, principalOptionSpec: ArgumentAcceptingOptionSpec[String]): Set[KafkaPrincipal] = {
    if (opts.options.has(principalOptionSpec))
      opts.options.valuesOf(principalOptionSpec).asScala.map(s => KafkaPrincipal.fromString(s.trim)).toSet
    else
      Set.empty[KafkaPrincipal]
  }

  private def getResource(opts: AclCommandOptions, dieIfNoResourceFound: Boolean = true): Set[Resource] = {
    var resources = Set.empty[Resource]
    if (opts.options.has(opts.topicOpt))
      opts.options.valuesOf(opts.topicOpt).asScala.foreach(topic => resources += new Resource(Topic, topic.trim))

    if (opts.options.has(opts.clusterOpt))
      resources += Resource.ClusterResource

    if (opts.options.has(opts.groupOpt))
      opts.options.valuesOf(opts.groupOpt).asScala.foreach(group => resources += new Resource(Group, group.trim))

    if (resources.isEmpty && dieIfNoResourceFound)
      CommandLineUtils.printUsageAndDie(opts.parser, "You must provide at least one resource: --topic <topic> or --cluster or --group <group>")

    resources
  }

  private def confirmAction(msg: String): Boolean = {
    println(msg)
    Console.readLine().equalsIgnoreCase("y")
  }

  private def validateOperation(opts: AclCommandOptions, resourceToAcls: Map[Resource, Set[Acl]]) = {
    for ((resource, acls) <- resourceToAcls) {
      val validOps = ResourceTypeToValidOperations(resource.resourceType)
      if ((acls.map(_.operation) -- validOps).nonEmpty)
        CommandLineUtils.printUsageAndDie(opts.parser, s"ResourceType ${resource.resourceType} only supports operations ${validOps.mkString(",")}")
    }
  }

  class AclCommandOptions(args: Array[String]) {
    val parser = new OptionParser
    val authorizerOpt = parser.accepts("authorizer", "Fully qualified class name of the authorizer, defaults to kafka.security.auth.SimpleAclAuthorizer.")
      .withRequiredArg
      .describedAs("authorizer")
      .ofType(classOf[String])
      .defaultsTo(classOf[SimpleAclAuthorizer].getName)

    val authorizerPropertiesOpt = parser.accepts("authorizer-properties", "REQUIRED: properties required to configure an instance of Authorizer. " +
      "These are key=val pairs. For the default authorizer the example values are: zookeeper.connect=localhost:2181")
      .withRequiredArg
      .describedAs("authorizer-properties")
      .ofType(classOf[String])

    val topicOpt = parser.accepts("topic", "topic to which ACLs should be added or removed. " +
      "A value of * indicates ACL should apply to all topics.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])

    val clusterOpt = parser.accepts("cluster", "Add/Remove cluster acls.")
    val groupOpt = parser.accepts("group", "Consumer Group to which the acls should be added or removed. " +
      "A value of * indicates the acls should apply to all groups.")
      .withRequiredArg
      .describedAs("group")
      .ofType(classOf[String])

    val addOpt = parser.accepts("add", "Indicates you are trying to add acls.")
    val removeOpt = parser.accepts("remove", "Indicates you are trying to remove acls.")
    val listOpt = parser.accepts("list", "List acls for the specified resource, use --topic <topic> or --group <group> or --cluster to specify a resource.")

    val upgradeAclsOpt = parser.accepts("upgradeAcls", "Indicates you are trying to migrate from older version of acl to newer version of acl. The migration only works if no resource exist" +
      "for which some acls are in older version and some are in newer.")
    val downgradeAclsOpt = parser.accepts("downgradeAcls", "Indicates you are trying to migrate from newer version of acl to older version of acl. The migration only works if no resource exist" +
      "for which some acls in older format already exits.")


    val operationsOpt = parser.accepts("operation", "Operation that is being allowed or denied. Valid operation names are: " + Newline +
      Operation.values.map("\t" + _).mkString(Newline) + Newline)
      .withRequiredArg
      .ofType(classOf[String])
      .defaultsTo(All.name)

    val allowPrincipalsOpt = parser.accepts("allow-principals", "Comma separated list of principals where principal is in principalType:name format." +
      " User:* is the wild card indicating all users.")
      .withRequiredArg
      .describedAs("allow-principals")
      .ofType(classOf[String])

    val denyPrincipalsOpt = parser.accepts("deny-principals", "Comma separated list of principals where principal is in " +
      "principalType: name format. By default anyone not in --allow-principals list is denied access. " +
      "You only need to use this option as negation to already allowed set. " +
      "For example if you wanted to allow access to all users in the system but not test-user you can define an acl that " +
      "allows access to User:* and specify --deny-principals=User:test@EXAMPLE.COM. " +
      "AND PLEASE REMEMBER DENY RULES TAKES PRECEDENCE OVER ALLOW RULES.")
      .withRequiredArg
      .describedAs("deny-principals")
      .ofType(classOf[String])

    val allowHostsOpt = parser.accepts("allow-host", "Host from which principals listed in --allow-principal will have access. " +
      "If you have specified --allow-principal then the default for this option will be set to * which allows access from all hosts.")
      .withRequiredArg
      .describedAs("allow-host")
      .ofType(classOf[String])

    val denyHostssOpt = parser.accepts("deny-host", "Host from which principals listed in --deny-principal will be denied access. " +
      "If you have specified --deny-principal then the default for this option will be set to * which denies access from all hosts.")
      .withRequiredArg
      .describedAs("deny-host")
      .ofType(classOf[String])

    val producerOpt = parser.accepts("producer", "Convenience option to add/remove acls for producer role. " +
      "This will generate acls that allows WRITE,DESCRIBE on topic and CREATE on cluster. ")

    val consumerOpt = parser.accepts("consumer", "Convenience option to add/remove acls for consumer role. " +
      "This will generate acls that allows READ,DESCRIBE on topic and READ on group.")

    val helpOpt = parser.accepts("help", "Print usage information.")

    val options = parser.parse(args: _*)

    def checkArgs() {
      CommandLineUtils.checkRequiredArgs(parser, options, authorizerPropertiesOpt)

      val actions = Seq(addOpt, removeOpt, listOpt, upgradeAclsOpt, downgradeAclsOpt).count(options.has)

      if (actions != 1)
        CommandLineUtils.printUsageAndDie(parser, "Command must include exactly one action: --list, --add, --remove, --upgradeAcls, --downgradeAcls. ")

      CommandLineUtils.checkInvalidArgs(parser, options, listOpt, Set(producerOpt, consumerOpt, allowHostsOpt, allowPrincipalsOpt, denyHostssOpt, denyPrincipalsOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, upgradeAclsOpt, Set(producerOpt, consumerOpt, allowHostsOpt, allowPrincipalsOpt, denyHostssOpt, denyPrincipalsOpt, clusterOpt, topicOpt, groupOpt, downgradeAclsOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, downgradeAclsOpt, Set(producerOpt, consumerOpt, allowHostsOpt, allowPrincipalsOpt, denyHostssOpt, denyPrincipalsOpt, clusterOpt, topicOpt, groupOpt, upgradeAclsOpt))

      //when --producer or --consumer is specified , user should not specify operations as they are inferred and we also disallow --deny-principals and --deny-hosts.
      CommandLineUtils.checkInvalidArgs(parser, options, producerOpt, Set(operationsOpt, denyPrincipalsOpt, denyHostssOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, consumerOpt, Set(operationsOpt, denyPrincipalsOpt, denyHostssOpt))

      if (options.has(producerOpt) && !options.has(topicOpt))
        CommandLineUtils.printUsageAndDie(parser, "With --producer you must specify a --topic")

      if (options.has(consumerOpt) && (!options.has(topicOpt) || !options.has(groupOpt) || (!options.has(producerOpt) && options.has(clusterOpt))))
        CommandLineUtils.printUsageAndDie(parser, "With --consumer you must specify a --topic and a --group and no --cluster option should be specified.")
    }
  }

}
