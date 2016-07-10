package com.gigaspaces.verifier

import java.util.concurrent.{TimeUnit, Executors, ScheduledFuture, ScheduledExecutorService}
import java.util.logging.Logger
import com.gigaspaces.common.{Verification, Data}
import org.openspaces.core.GigaSpace
import org.openspaces.core.context.GigaSpaceContext
import org.springframework.beans.factory._
import org.openspaces.scala.core.ScalaGigaSpacesImplicits._
import org.openspaces.scala.core.aliases.annotation._


class Verifier extends InitializingBean with DisposableBean {

  @GigaSpaceContext private var gigaSpace: GigaSpace = _

  private val log: Logger = Logger.getLogger(this.getClass.getName)

  private val executorService: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  private var sf: ScheduledFuture[_] = _

  private val defaultDelay: Long = 10000

  private val engine: VerifierEngine = new VerifierEngine

  def afterPropertiesSet(): Unit = {
    log.info(s"--- VERIFIER - after properties set")

    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        val unverifiedData = gigaSpace.predicate.readMultiple { data: Data => data.processed == true && data.verified == false }

        log.info(s"Processed and unverified data count = ${unverifiedData.length}")

        unverifiedData.foreach { data: Data =>
          engine.isVerified(data) match {
            case true => {
              data.setVerified(true)
              val verificationId = s"verification-${data.getId}".replace("^", "")
              gigaSpace.write(Verification(verificationId, data.getId))
              gigaSpace.write(data)
              log.info(s"POSITIVE VERIFICATION")
            }
            case false => {
              gigaSpace.clear(data)
              log.info(s"NEGATIVE VERIFICATION")
            }
          }
        }
      }
    })

    log.info(s"--- STARTING VERIFIER WITH CYCLE [${defaultDelay}]")

    sf = executorService.scheduleAtFixedRate(thread, defaultDelay, defaultDelay, TimeUnit.MILLISECONDS)
  }

  def destroy(): Unit = {
    sf.cancel(false)
    sf = null
    executorService.shutdown
  }

}
