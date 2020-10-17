# spark-ml
Spark examples, Java, Scala, Python, and Jupyter Notebooks

## In this project
You will find here:
- A `Dockerfile` allowing you to build and run a docker image containing
    - A runnable instance Spark-Hadoop-Hive
    - Python 3
    - Java
    - Scala
    - Jupyter notebooks server (for Python3, Java, Scala)
    - This project, with
        - Jupyter Notebooks, for Python3, Java, Scala
        - Java code
        - Python3 code
        - Scala code
- All the resources of this project will be runnable in the docker image built above.     

## Cloudera - Spark class, Oct-2020
The goal is to reproduce what was done during the Cloudera class I took in Oct-2020, without Cloudera Data Science Workbench (CDSW).

The docker image we will need can be built as indicated above.
The script `build.image.sh` will do the job.

Once built, run a new container:
```
$ docker run -it --rm -e USER=root -p 8080:8080 oliv-spark:latest /bin/bash
``` 
The port (`-p 8080:8080`) is for Jupyter.

#### To transfer extra data to the docker image
If needed:
```
$ docker ps
CONTAINER ID        IMAGE               COMMAND             CREATED             STATUS              PORTS                    NAMES
df364841cbd9        oliv-spark:latest   "/bin/bash"         9 minutes ago       Up 9 minutes        0.0.0.0:8080->8080/tcp   vigorous_brown
$
$ docker cp ~/Desktop/.../spark-ml/duocar-raw-part-01.zip df364841cbd9:/workdir/spark-3.0.1-bin-hadoop2.7-hive1.2/ai-data.zip
$
```

Then, in the docker image, unzip the data:
```
$ cd spark-3.0.1-bin-hadoop2.7-hive1.2
$ unzip ai-data.zip
. . .
```

## Basic validations
We just want here to validate the Spark basics (availability).

In the docker container, do the following:

#### In Python3
```
$ ./bin/pyspark
```
> Note: Make sure you've modified the `bin/pyspark` so it uses `python3`.

Execute the following lines:

_Optional:_
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("spark-test").getOrCreate()
```
Then
```python
FILE_LOCATION = "file:///workdir/spark-3.0.1-bin-hadoop2.7-hive1.2/spark-ml/duocar/raw/rides/"
rides = spark.read.csv(FILE_LOCATION, \
                       sep=",", \
                       header=True, \
                       inferSchema=True)
rides.printSchema()
```

The `printSchema` should return
```
root
 |-- id: integer (nullable = true)
 |-- driver_id: long (nullable = true)
 |-- rider_id: long (nullable = true)
 |-- date_time: string (nullable = true)
 |-- utc_offset: integer (nullable = true)
 |-- service: string (nullable = true)
 |-- origin_lat: double (nullable = true)
 |-- origin_lon: double (nullable = true)
 |-- dest_lat: double (nullable = true)
 |-- dest_lon: double (nullable = true)
 |-- distance: integer (nullable = true)
 |-- duration: integer (nullable = true)
 |-- cancelled: integer (nullable = true)
 |-- star_rating: integer (nullable = true)

```

#### Same in Scala
In `./bin/spark-shell`, run 
```scala
val FILE_LOCATION = "file:///workdir/spark-3.0.1-bin-hadoop2.7-hive1.2/spark-ml/duocar/raw/rides/"
val rides = spark.read
                 .option("delimiter", ",")
                 .option("inferSchema", true)
                 .option("header", true)
                 .csv(FILE_LOCATION)
rides.count()
rides.printSchema()
```

```
root@f43725f78e97:/workdir/spark-3.0.1-bin-hadoop2.7-hive1.2# ./bin/spark-shell
. . .
Spark context available as 'sc' (master = local[*], app id = local-1602513424078).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.0.1
      /_/
         
Using Scala version 2.12.10 (OpenJDK 64-Bit Server VM, Java 11.0.8)
Type in expressions to have them evaluated.
Type :help for more information.


scala> val rides = spark.read.option("delimiter", ",").option("inferSchema", true).option("header", true).csv("file:///workdir/spark-3.0.1-bin-hadoop2.7-hive1.2/spark-ml/duocar/raw/rides/")
rides: org.apache.spark.sql.DataFrame = [id: int, driver_id: bigint ... 12 more fields]

scala> rides.count()
res0: Long = 48775

scala> rides.printSchema()
root
 |-- id: integer (nullable = true)
 |-- driver_id: long (nullable = true)
 |-- rider_id: long (nullable = true)
 |-- date_time: string (nullable = true)
 |-- utc_offset: integer (nullable = true)
 |-- service: string (nullable = true)
 |-- origin_lat: double (nullable = true)
 |-- origin_lon: double (nullable = true)
 |-- dest_lat: double (nullable = true)
 |-- dest_lon: double (nullable = true)
 |-- distance: integer (nullable = true)
 |-- duration: integer (nullable = true)
 |-- cancelled: integer (nullable = true)
 |-- star_rating: integer (nullable = true)

scala> 
```
#### Java & Scala from the command line
Along the same lines, you can do, from the `spark-ml` directory:
```
$ ./gradlew shadowJar
$ java -cp build/libs/spark-ml-1.0-all.jar javasamples.Scaffolding
. . .

$ scala -cp build/libs/spark-ml-1.0-all.jar scalasamples.Scaffolding
. . .
```

Good!
 
## Other resources and docs
- [JUPYTER.md](./JUPYTER.md)
- [SPARK_DEBIAN.md](./SPARK_DEBIAN.md)
- Good Java-Spark examples [here](https://www.programcreek.com/java-api-examples/?code=IsaacChanghau%2FStockPrediction%2FStockPrediction-master%2Fsrc%2Fmain%2Fjava%2Fcom%2Fisaac%2Fstock%2Futils%2FDataPreview.java#)

---
 