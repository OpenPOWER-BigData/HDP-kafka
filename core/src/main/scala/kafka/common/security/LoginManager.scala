/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.common.security

import org.apache.kafka.common.security.kerberos.Login
import org.apache.kafka.common.security.JaasUtils
import javax.security.auth.Subject
import java.util.concurrent._
import atomic.AtomicBoolean
import kafka.utils.Logging

object LoginManager extends Logging {
  var login: Login = null
  var serviceName: String = null
  var loginContext: String = null
  var isStarted = new AtomicBoolean(false)

  def init(loginContext:String, configs: java.util.Map[String, _]) {
    if(isStarted.compareAndSet(false, true)) {
      this.loginContext = loginContext
      login = new Login(loginContext, configs)
      login.startThreadIfNeeded()
      serviceName = JaasUtils.jaasConfig(loginContext, JaasUtils.SERVICE_NAME)
    }
  }

  def subject : Subject = {
    if(isStarted.get())
      return login.subject
    null
  }


  def shutdown {
    if (login != null) {
      isStarted.set(false)
      login.shutdown()
    }
  }

}
