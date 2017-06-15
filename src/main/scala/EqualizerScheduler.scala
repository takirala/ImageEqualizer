import java.awt.Color
import java.awt.image.BufferedImage
import java.io.File
import java.nio.ByteBuffer
import java.util
import java.util.function.IntBinaryOperator
import javax.imageio.ImageIO

import com.google.protobuf.ByteString
import org.apache.mesos.Protos._
import org.apache.mesos._

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}

/**
  * Created by takirala on 6/12/17.
  */
class EqualizerScheduler(executor: ExecutorInfo, _srcImage: String = "unequalized1.jpg", _maxBins: Int = 100, _memPerTask: Double = 256, _outputPrefix: String = "/vagrant/") extends Scheduler {

  /**
    * TODO executorID mapping
    * TODO lost task tracker relaunch.
    *
    */

  private val _map = mutable.Map[Int, Int]()

  private val _cpuPerTask: Double = 0.1
  private val _binQueue: mutable.Queue[Int] = mutable.Queue((1 to _maxBins).toArray: _*)
  private val _inProgress: mutable.Set[Int] = mutable.Set()

  private def getCpuCount(offer: Offer): Double = {
    val cpus = offer.getResourcesList.asScala.filter(_.getName == "cpus")
    assert(cpus.length == 1)
    cpus.head.getScalar.getValue
  }

  private def getTotalMem(offer: Offer): Double = {
    val mem = offer.getResourcesList.asScala.filter(_.getName == "mem")
    assert(mem.length == 1)
    mem.head.getScalar.getValue
  }

  private def getMaxTasks(cpus: Double, mem: Double): Int = {
    val maxByCPU = cpus / _cpuPerTask
    val maxByRAM = mem / _memPerTask
    Math.min(maxByCPU, maxByRAM).toInt
  }

  private def getTasks(offer: Offer, MAX_TASKS: Int): immutable.Seq[TaskInfo] = {

    for (_ <- 1 to MAX_TASKS if _binQueue.nonEmpty) yield {
      val binIndex = _binQueue.dequeue()
      val byteBuffer = ByteBuffer.allocate(8).putInt(0, binIndex).putInt(4, _maxBins)

      val id = TaskID.newBuilder.setValue(s"iTask ${System.currentTimeMillis()} - bin - $binIndex")
      val name = s"Task-${id.getValue}"

      val cpuResource = Resource.newBuilder
        .setName("cpus")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder.setValue(_cpuPerTask))

      val memResource = Resource.newBuilder
        .setName("mem")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder.setValue(_memPerTask))

      TaskInfo.newBuilder
        .setData(ByteString.copyFrom(byteBuffer))
        .setExecutor(executor)
        .setName(name)
        .setTaskId(id)
        .setSlaveId(offer.getSlaveId)
        .addResources(cpuResource)
        .addResources(memResource)
        .build()
    }
  }

  override def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {

    println(s"Received ${offers.size()} offers!")

    for (offer <- offers.asScala) {
      println(s"Offer ${offer.getId.getValue}")
      //println(offer.toString)
      try {
        val MAX_TASKS = getMaxTasks(getCpuCount(offer), getTotalMem(offer))
        val nextTask = getTasks(offer, MAX_TASKS)
        if (nextTask.nonEmpty) {
          println(s"Launching tasks ${nextTask.length}")
          driver.launchTasks(List(offer.getId).asJava, nextTask.asJava)
        } else {
          println(s"Declining offer with id ${offer.getId} as there are no more tasks to run")
          driver.declineOffer(offer.getId)
        }
      } catch {
        case e: AssertionError =>
          println(s"Declining offer as ${e.getMessage} there are not enough resources in ${offer.getResourcesList.asScala.filter(_.getName == "mem").toString()}")
          driver.declineOffer(offer.getId)
      }
    }
  }

  override def statusUpdate(driver: SchedulerDriver, status: Protos.TaskStatus): Unit = {

    if (status.getState == TaskState.TASK_RUNNING) {
      //Don't do anything as of now. TODO
      _inProgress += extractBinIndex(status.getTaskId.getValue)
    } else if (status.getState == TaskState.TASK_FINISHED) {
      val byteBuffer = status.getData.asReadOnlyByteBuffer()
      val binIndex = byteBuffer.getInt()
      val frequency = byteBuffer.getInt()
      _inProgress.remove(binIndex)
      _map(binIndex - 1) = frequency
      println(s"Task Finished ${extractBinIndex(status.getTaskId.getValue)} Finished : ${_map.size} In progress : ${_inProgress.size}")
    } else {
      println(s"Status update : ${status.getState} for task id ${status.getTaskId}")
      _binQueue += extractBinIndex(status.getTaskId.getValue)
    }

    if (_map.size == _maxBins) {
      // Done. Proceed further.
      createRGBImage()
      val beer = """\uD83C\uDF7A \uD83C\uDF7A \uD83C\uDF7A \uD83C\uDF7A \uD83C\uDF7A \uD83C\uDF7A"""
      val doneMessage =
        s"""
           |
           |                  .-'''-.                                       ___
           |_______         '   _    \\                                  .'/   \\
           |\\  ___ `'.    /   /` '.   \\    _..._         __.....__     / /     \\
           | ' |--.\\  \\  .   |     \\  '  .'     '.   .-''         '.   | |     |
           | | |    \\  ' |   '      |  '.   .-.   . /     .-''"'-.  `. | |     |
           | | |     |  '\\    \\     / / |  '   '  |/     /________\\   \\|/`.   .'
           | | |     |  | `.   ` ..' /  |  |   |  ||                  | `.|   |
           | | |     ' .'    '-...-'`   |  |   |  |\\    .-------------'  ||___|
           | | |___.' /'                |  |   |  | \\    '-.____...---.  |/___/
           |/_______.'/                 |  |   |  |  `.             .'   .'.--.
           |\\_______|/                  |  |   |  |    `''-...... -'    | |    |
           |                            |  |   |  |                     \\_\\    /
           |                            '--'   '--'                      `''--'
         """.stripMargin
      println(beer)
      //println(doneMessage)
      driver.stop()
    }
  }

  private def extractBinIndex(taskId: String): Int = {
    taskId.split("- bin - ")(1).toInt
  }

  private def createRGBImage(): Unit = {

    val image = ImageIO.read(getClass.getResource(_srcImage))
    val w = image.getWidth()
    val h = image.getHeight()
    val rgbArray = image.getRGB(0, 0, w, h, null, 0, w)
    val hsbArray = rgbArray.map(x => {
      val c = new Color(x)
      Color.RGBtoHSB(c.getRed, c.getGreen, c.getBlue, null)
    }
    )

    val binFreq = Array.fill(_maxBins)(0)
    _map.map(x => {
      binFreq(x._1) = x._2
    })
    util.Arrays.parallelPrefix(binFreq, new IntBinaryOperator {
      override def applyAsInt(left: Int, right: Int): Int = left + right
    }
    )
    val cumProb = Array.fill(_maxBins)(0.0f)
    for (i <- 0 until _maxBins) {
      cumProb(i) = binFreq(i).toFloat / hsbArray.length
    }

    val newRgbArray = hsbArray.map(x => {
      val brightness = x(2)
      val binIndex = (brightness * (_maxBins - 1)).toInt
      Color.HSBtoRGB(x(0), x(1), cumProb(binIndex))
    })

    val newImage = new BufferedImage(w, h, image.getType)
    val output = new File(_outputPrefix + _srcImage + ".output.jpg")
    newImage.setRGB(0, 0, image.getWidth(), image.getHeight(), newRgbArray, 0, image.getWidth())
    ImageIO.write(newImage, "jpg", output)
  }

  override def registered(driver: SchedulerDriver, frameworkId: Protos.FrameworkID, masterInfo: Protos.MasterInfo): Unit = {
    println("Registered")
  }

  override def error(driver: SchedulerDriver, message: String): Unit = {
    println(s"Error : $message")
  }

  override def offerRescinded(driver: SchedulerDriver, offerId: Protos.OfferID): Unit = {
    println(s"Offer rescinded ${offerId.toString}")
  }

  override def disconnected(driver: SchedulerDriver): Unit = {
    println("Driver disconnected")
  }

  override def slaveLost(driver: SchedulerDriver, slaveId: Protos.SlaveID): Unit = {
    println(s"slave lost $slaveId")
  }

  override def reregistered(driver: SchedulerDriver, masterInfo: Protos.MasterInfo): Unit = {
    println("reregistered")
  }

  override def frameworkMessage(driver: SchedulerDriver, executorId: Protos.ExecutorID, slaveId: Protos.SlaveID, data: Array[Byte]): Unit = {
    println(s"Message from framework $data")
  }

  override def executorLost(driver: SchedulerDriver, executorId: Protos.ExecutorID, slaveId: Protos.SlaveID, status: Int): Unit = {
    println(s"executor lost with status $status")
  }
}
