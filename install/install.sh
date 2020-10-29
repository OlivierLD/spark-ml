#!/bin/bash
#
# Standalone of the Docker image...
#
SCALA_VERSION=2.12.12
SCALA_TARBALL=http://www.scala-lang.org/files/archive/scala-${SCALA_VERSION}.deb

# See https://spark.apache.org/downloads.html
# ENV APACHE_MIRROR https://mirrors.gigenet.com/apache/spark/spark-3.0.1/
APACHE_MIRROR=https://mirrors.ocf.berkeley.edu/apache/spark/spark-3.0.1/
# ENV APACHE_MIRROR https://mirrors.gigenet.com/apache/spark/spark-3.0.0-preview2/
# ENV SPARK_TARBALL http://apache.claz.org/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz
# ENV SPARK_TARBALL $APACHE_MIRROR/spark-3.0.0-preview2-bin-hadoop3.2.tgz
SPARK_TARBALL=${APACHE_MIRROR}/spark-3.0.1-bin-hadoop2.7-hive1.2.tgz
#
sudo apt-get update
sudo apt-get install -y curl git build-essential default-jdk libssl-dev libffi-dev python-dev vim zip
sudo apt-get install -y python-pip python-dev
sudo apt-get install -y python3-pip python3-dev python3-venv
sudo pip3 install pandas numpy scipy scikit-learn
#
sudo apt-get install -y cmake unzip pkg-config libopenblas-dev liblapack-dev
sudo apt-get install -y python-numpy python-scipy python-matplotlib python-yaml
sudo python3 -mpip install matplotlib
#
sudo pip3 install jupyter
sudo pip3 install pyspark
#
sudo apt-get install -y python-opencv
sudo apt-get install -y python3-tk
#
# SDK, for SBT and others
sudo curl -s "https://get.sdkman.io" | bash
#
sudo /bin/bash "$HOME/.sdkman/bin/sdkman-init.sh"
echo "+-----------------------+"
echo "| ===> installing Scala |"
echo "+-----------------------+"
sudo apt-get install -y --force-yes libjansi-java &&
     curl -sSL $SCALA_TARBALL -o scala.deb &&
     dpkg -i scala.deb &&
     echo "===> Cleaning up..." &&
     rm -f *.deb
#
# RUN sdk install sbt
#
mkdir -p ~/workdir
cd ~/workdir
echo "+-----------------------+" &&
echo "| ===> installing Spark |" &&
echo "+-----------------------+" &&
  curl -sSL $SPARK_TARBALL -o spark.tgz &&
  tar xvf spark.tgz &&
  echo "===> Cleaning up..." &&
  rm spark.tgz
#
# TODO Hive, Hadoop, etc?
#
cd ~/workdir/spark-3.0.1-bin-hadoop2.7-hive1.2
git clone https://github.com/OlivierLD/spark-ml.git
#
echo -e "Try that!"
