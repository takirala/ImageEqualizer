import java.awt.Color
import java.awt.image.BufferedImage
import java.io.File
import java.util
import java.util.function._
import javax.imageio.ImageIO

/**
  * Created by takirala on 6/8/17.
  */
object SingleEqualizer {

  val srcImage = "unequalized2.jpg"
  val nBins = 10

  def main(args: Array[String]): Unit = {
    println("Working")

    //Load the image as an array
    var rgbArray = loadImage()
    println(s"loaded image with number of pixels ${rgbArray.length}")

    //process the rgb array to hsb format in parallel
    var hsbArray = convertRGBToHSB(rgbArray)
    println(s"Converted to hsb with size ${hsbArray.length}")

    //create brightness map in a word count fashion
    var bHist = getBrightNessMap(hsbArray)
    println("map calculated")

    //probability array - has to be serial ?
    var bins = getFrequencyForEachBin(bHist)
    println("frequency populated")

    //parallel prefix. single node?
    var cumProb = calculateCumProb(bins, rgbArray.length)
    println("calculated cummulative probability")

    //equalize pixels in parallel
    var newRgbArray = getRGBFromHSB(hsbArray, cumProb)
    println("computed new rgb array")

    //create new image.
    write_to_new_file(newRgbArray)
    println("written to a new array")
    println("Done!")
  }

  private def loadImage(): Array[Int] = {
    // println(System.getProperty("user.dir"))
    val image = ImageIO.read(getClass.getResource(srcImage))
    val w = image.getWidth()
    val h = image.getHeight()
    image.getRGB(0, 0, w, h, null, 0, w)
  }

  // Returns aa 2d float array
  private def convertRGBToHSB(rgbArray: Array[Int]): Array[Array[Float]] = {
    val len = rgbArray.length
    rgbArray.map(x => {
      var c = new Color(x)
      Color.RGBtoHSB(c.getRed, c.getGreen, c.getBlue, null)
    })
  }

  private def getBrightNessMap(hsbArray: Array[Array[Float]]): Seq[(Int, Int)] = {

    //println(hsbArray.count(x => ((x(2) * (nBins - 1)).toInt) == 51))

    hsbArray.map(x => x(2))
      .groupBy(x => (x * (nBins - 1)).toInt)
      .mapValues(_.length).toSeq
  }

  private def getFrequencyForEachBin(bHist: Seq[(Int, Int)]): Array[Int] = {
    val binFreq = Array.fill(nBins)(0)
    bHist.map(x => {
      binFreq(x._1) = x._2
    })
    binFreq
  }

  private def calculateCumProb(bins: Array[Int], numOfPixels: Int): Array[Float] = {
    util.Arrays.parallelPrefix(bins, new IntBinaryOperator {
      override def applyAsInt(left: Int, right: Int): Int = left + right
    })
    val cumProb = Array.fill(nBins)(0.0f)
    for (i <- 0 until nBins) {
      cumProb(i) = bins(i).toFloat / numOfPixels
    }
    cumProb
  }

  private def getRGBFromHSB(hsbArray: Array[Array[Float]], cumProb: Array[Float]): Array[Int] = {
    hsbArray.map(x => {
      val brightness = x(2)
      val binIndex = (brightness * (nBins - 1)).toInt
      //x(2) = cumProb(binIndex)
      Color.HSBtoRGB(x(0), x(1), cumProb(binIndex))
    })
  }

  private def write_to_new_file(newRgbArray: Array[Int]): Boolean = {
    val image = ImageIO.read(getClass.getResource(srcImage))
    var newImage = new BufferedImage(image.getWidth(), image.getHeight(), image.getType)
    val output = new File(srcImage + ".output.jpg")
    newImage.setRGB(0, 0, image.getWidth(), image.getHeight(), newRgbArray, 0, image.getWidth())
    ImageIO.write(newImage, "jpg", output)
  }
}
