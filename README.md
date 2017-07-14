# Histogram Equalization

## The what and why.

Wikipedia defines it as *Histogram equalization is a method in image processing of contrast adjustment using the image's histogram.*

* This method usually increases the global contrast of many images, especially when the usable data of the image is represented by close contrast values. 
* Through this adjustment, the intensities can be better distributed on the histogram. 
* This allows for areas of lower local contrast to gain a higher contrast. Histogram equalization accomplishes this by effectively spreading out the most frequent intensity values.

I am sure that didn't make much sense. In essence, this code helps convert the image on left to image on right :


| Original Image | After Equalization (10 bins) | After Equalization (100 bins) |
| --- | --- | --- |
| ![Before](/src/main/resources/unequalized1.jpg)  | ![Before](/src/main/resources/unequalized1-10bins.jpg) | ![Before](/src/main/resources/unequalized1-100bins.jpg) |


## Okay, but how ?

* Any image is RGB
* Convert to HSB
* Collect the**B**values in to bins (Does the bin size matter?)
* Calculate the frequency of each bin (Hence the name **Histogram** Equalization)
* Calculate the cumulative probability of each bin
* Over ride the corresponding **B** for the bin with its cumulative probability in the image
* Convert back to RGB write the new image!

## What's the role of Mesos?

Images that we use can range anywhere from 500K pixels to 8B pixels. Processing each pixels requires significant computing power. In this specific use case, a cluster is spun up and the master is used to run the scheduler. The scheduler create tasks where in each task is assigned a specific bin to compute the overall frequency of its presence in the image. When a task is successfully completed, the master gets the status update which has the frequency information of the specific bin.

Mesos helps delegate the tasks parallely (parallel being the keyword) to multiple slave nodes. As computing the frequency of each bin is independent of each other, mesos makes is super fast. Once all the frequencies are collected at master, further computation happens in the scheduler to create the new image.

## How to run?

First and foremost you need to set up few things to be able to run this.

### What do I need to run ?

* You need to have access to a Mesos cluster (I prefer to install it locally).
* JVM (>= 1.8)
* SBT (Tested with 0.13.15)

Now that you have it set up, below are the instructions to hack the image:

`Note : In my current set up, /vagrant is a shared directory that is accesible every where`

* Clone this repository and do a `sbt assembly` in the directory. You should be able to see an assembled jar in the desired directory (Controlled using a property in **build.sbt**). 
* Copy of the above jar to /vagrant directory. You can copy this anywhere but make sure you change it everywhere relavant.
* SSH to a node that can talk to the master.
* Enter `java -cp /vagrant/ImageEqualizer-assembly-1.0.jar Equalizer` to start the main program. 
* Grab a 🍺 and watch the magic. Your output should be written to /vagrant
