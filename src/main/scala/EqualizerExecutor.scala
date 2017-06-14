import java.awt.Color
import java.nio.ByteBuffer
import java.util.concurrent.{ExecutorService, Executors}
import javax.imageio.ImageIO

import com.google.protobuf.ByteString
import org.apache.mesos.Protos.{TaskState, TaskStatus}
import org.apache.mesos.{Executor, ExecutorDriver, MesosExecutorDriver, Protos}

/**
  * Created by takirala on 6/12/17.
  */
object EqualizerExecutor extends Executor {

  val fileName = "unequalized.jpg"
  val poolSize = 50

  private var hsbArray = Array[Array[Float]]()
  private val pool: ExecutorService = Executors.newCachedThreadPool()

  override def registered(driver: ExecutorDriver, executorInfo: Protos.ExecutorInfo, frameworkInfo: Protos.FrameworkInfo, slaveInfo: Protos.SlaveInfo): Unit = {
    println("registered")
  }

  bootstrap()

  class WorkerThread(driver: ExecutorDriver, task: Protos.TaskInfo) extends Runnable {
    override def run(): Unit = {
      driver.sendStatusUpdate(
        TaskStatus.newBuilder.setTaskId(task.getTaskId)
          .setState(TaskState.TASK_RUNNING)
          .build()
      )

      val buffer = task.getData.asReadOnlyByteBuffer()
      val myBinIndex = buffer.getInt()
      val maxBins = buffer.getInt()

      println(s"Read from task data as myBinIndex - $myBinIndex and maxBins - $maxBins")

      val frequency = hsbArray.count(x => (x(2) * (maxBins - 1)).toInt == myBinIndex)

      val byteBuffer = ByteBuffer.allocate(8)
      byteBuffer.putInt(0, myBinIndex)
      byteBuffer.putInt(4, frequency)

      println(s"Sending status update for $myBinIndex with frequency as $frequency using bytebuffer $byteBuffer")

      driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(task.getTaskId)
        .setState(TaskState.TASK_FINISHED)
        .setData(ByteString.copyFrom(byteBuffer))
        .build())
    }
  }

  override def launchTask(driver: ExecutorDriver, task: Protos.TaskInfo): Unit = {
    println("Hello there, task has been launched.")
    pool.execute(new WorkerThread(driver, task))
  }

  def main(args: Array[String]): Unit = {
    val driver = new MesosExecutorDriver(EqualizerExecutor.this)
    driver.run()
  }

  override def error(driver: ExecutorDriver, message: String): Unit = println("error")

  override def killTask(driver: ExecutorDriver, taskId: Protos.TaskID): Unit = println("task killed")

  override def disconnected(driver: ExecutorDriver): Unit = println("disconnected")

  override def reregistered(driver: ExecutorDriver, slaveInfo: Protos.SlaveInfo): Unit = println("reregisterd")

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = println(s"framework message $data")

  override def shutdown(driver: ExecutorDriver): Unit = {
    println("shutdown")
    pool.shutdownNow()
  }

  private def bootstrap(): Unit = {
    println(s"Booting up!! from ${System.getProperty("user.dir")} ")
    try {
      val image = ImageIO.read(getClass.getResource(fileName))
      val w = image.getWidth()
      val h = image.getHeight()
      val rgbArray = image.getRGB(0, 0, w, h, null, 0, w)
      hsbArray = rgbArray.map(x => {
        val c = new Color(x)
        Color.RGBtoHSB(c.getRed, c.getGreen, c.getBlue, null)
      })
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}
