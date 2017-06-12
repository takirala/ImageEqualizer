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

  val srcImage = "images/unequalized.jpg"
  val nBins = 100 //For precision.

  def getImage() = {
    // println(System.getProperty("user.dir"))
    val image = ImageIO.read(new File(srcImage))
    val w = image.getWidth()
    val h = image.getHeight()
    image.getRGB(0, 0, w, h, null, 0, w)
  }

  // Returns aa 2d float array
  def convertRGBToHSB(rgbArray: Array[Int]) = {
    val len = rgbArray.length
    rgbArray.map(x => {
      var c = new Color(x)
      Color.RGBtoHSB(c.getRed, c.getGreen, c.getBlue, null)
    })
  }

  def getBrightNessMap(hsbArray: Array[Array[Float]]): Seq[(Int, Int)] = {
    hsbArray.map(x => x(2))
      .groupBy(x => (x * (nBins-1)).toInt)
      .mapValues(_.size).toSeq
  }

  def getFrequencyForEachBin(bHist: Seq[(Int, Int)]) = {
    val binFreq = Array.fill(nBins){0}
    bHist.map(x => {
      binFreq(x._1) = x._2
    })
    binFreq
  }

  def calculateCumProb(bins: Array[Int], numOfPixels : Int) = {
    util.Arrays.parallelPrefix(bins,new IntBinaryOperator {
      override def applyAsInt(left: Int, right: Int): Int = left+right
    })
    var cumProb = Array.fill(nBins){0.0f}
    for(i <- 0 until nBins) {
      cumProb(i) = bins(i).toFloat / numOfPixels
    }
    cumProb
  }

  def mapHSB(hsbArray: Array[(Float, Float, Float)], cumProb: Array[Float]): Array[(Float, Float, Float)] = ???

  def getRGBFromHSB(hsbArray: Array[Array[Float]], cumProb : Array[Float]) = {
    hsbArray.map( x => {
      val brightness = x(2)
      val binIndex = (brightness * (nBins-1)).toInt
      //x(2) = cumProb(binIndex)
      Color.HSBtoRGB(x(0), x(1), cumProb(binIndex))
    })
  }

  def write_to_new_file(newRgbArray: Array[Int]) = {
    val image = ImageIO.read(new File(srcImage))
    var newImage = new BufferedImage(image.getWidth(), image.getHeight(), image.getType)
    val output = new File(srcImage+".output.jpg")
    newImage.setRGB(0, 0, image.getWidth(), image.getHeight(), newRgbArray, 0, image.getWidth())
    ImageIO.write(newImage, "jpg", output)
  }

  def main(args: Array[String]): Unit = {
    println("Hello")

    //Load the image as an array
    var rgbArray = getImage()

    //process the rgb array to hsb format in parallel
    var hsbArray = convertRGBToHSB(rgbArray)

    //create brightness map in a word count fashion
    var bHist = getBrightNessMap(hsbArray)

    //probability array - has to be serial ?
    var bins = getFrequencyForEachBin(bHist)
    println(bins.getClass)

    //parallel prefix. single node?
    var cumProb = calculateCumProb(bins, rgbArray.length)

    //equalize pixels in parallel
    var newRgbArray = getRGBFromHSB(hsbArray, cumProb)

    //create new image.
    write_to_new_file(newRgbArray)
    println("Done!")
  }
}
