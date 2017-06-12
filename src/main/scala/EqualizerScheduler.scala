import java.util

import org.apache.mesos.Protos._
import org.apache.mesos._

import scala.collection.JavaConverters._

/**
  * Created by takirala on 6/12/17.
  */
class EqualizerScheduler extends Scheduler {


  override def statusUpdate(driver: SchedulerDriver, status: Protos.TaskStatus): Unit = ???

  override def registered(driver: SchedulerDriver, frameworkId: Protos.FrameworkID, masterInfo: Protos.MasterInfo): Unit = ???

  override def error(driver: SchedulerDriver, message: String): Unit = ???

  override def offerRescinded(driver: SchedulerDriver, offerId: Protos.OfferID): Unit = ???

  override def disconnected(driver: SchedulerDriver): Unit = ???

  override def slaveLost(driver: SchedulerDriver, slaveId: Protos.SlaveID): Unit = ???

  override def reregistered(driver: SchedulerDriver, masterInfo: Protos.MasterInfo): Unit = ???

  override def frameworkMessage(driver: SchedulerDriver, executorId: Protos.ExecutorID, slaveId: Protos.SlaveID, data: Array[Byte]): Unit = ???

  override def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {

    for (offer <- offers.asScala) {
      println("Received offer with ID: ${offer.getId.getValue}")
      println("Declining offer ID ${offer.getId.getValue}")
      driver.declineOffer(offer.getId)
    }
  }

  override def executorLost(driver: SchedulerDriver, executorId: Protos.ExecutorID, slaveId: Protos.SlaveID, status: Int): Unit = ???
}
