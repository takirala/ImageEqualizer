import org.apache.mesos.MesosSchedulerDriver
import org.apache.mesos.Protos._

/**
  * Created by takirala on 6/12/17.
  */
object Equalizer {


  val usage = "Usage: Executor [-master IP:PORT] [-max Int] [-file String] [-mem Double] [-output String]"

  def main(args: Array[String]): Unit = {

    println(s"Override defaults with $usage")

    val name = "ImageEqualizer"

    val mesosMaster = "192.168.65.90:5050"

    val _srcImage = "unequalized1.jpg"
    val _outputPrefix = "/vagrant/"
    val _maxBins = 100
    val _memPerTask: Double = 256

    val id = FrameworkID.newBuilder.setValue(name).build()

    val executorCommand = CommandInfo.newBuilder()
      .setValue("/opt/mesosphere/bin/java -cp /vagrant/ImageEqualizer-assembly-1.0.jar EqualizerExecutor").build()

    val executorId = ExecutorID.newBuilder.setValue("EqualizerExecutor-" + System.currentTimeMillis())

    val executor = ExecutorInfo.newBuilder()
      .setCommand(executorCommand)
      .setExecutorId(executorId)
      .setName(name)
      .build()

    val scheduler = new EqualizerScheduler(executor, _srcImage, _maxBins, _memPerTask, _outputPrefix)

    val framework = FrameworkInfo.newBuilder
      .setName(name)
      .setUser("")
      .build()

    val driver = new MesosSchedulerDriver(scheduler, framework, mesosMaster)
    driver.run()
  }
}