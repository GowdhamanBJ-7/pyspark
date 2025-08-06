import os

# Set Hadoop home directory
os.environ["HADOOP_HOME"] = "C:\\hadoop\\hadoop-3.3.4"

# Add Hadoop bin directory to system PATH
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\hadoop-3.3.4\\bin"
