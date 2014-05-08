#pyADAM

This is a wrapper to load Parquet data in PySpark

## FAQ

### How does this work?

This uses a java class ParquetContext to handle loading of Parquet data and serializing (currently just as JSON).  The python script `parquet.py` creates an instance of ParquetContext and then issues a call to load in data a particular path.  This creates a backing Java RDD of JSON.

### Why is there a copy of `load_gateway` and `spark-class`
Currently pySpark launches a JVM by using py4j.  It puts the spark jars on the class path by first assembling them in spark-class.  For our custom application, we must also place our JAR along with the Spark jars on the classpath. This spark-class is slightly altered to allow users to specify additional JARs to place on the classpath before launching.  The `load_gateway` script is altered in two ways 1) to use this spark-class script and 2) to import JARs.  The latter step is not necessary as the classes can always be referred to with their FQN or imported later.
