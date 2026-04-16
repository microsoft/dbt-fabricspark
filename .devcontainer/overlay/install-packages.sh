#!/usr/bin/env -S bash -e

export ACCEPT_EULA=Y
export DEBIAN_FRONTEND=noninteractive
export OS_DISTRIBUTION=$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2)
export SCRIPT_DIR=$(realpath $(dirname $0))
source "${SCRIPT_DIR}/common.sh"

curl -sSL -O "https://packages.microsoft.com/config/ubuntu/${OS_DISTRIBUTION}/packages-microsoft-prod.deb"
sudo dpkg -i packages-microsoft-prod.deb >/dev/null
rm -f packages-microsoft-prod.deb

apt-get update
apt-get install -y \
    apt-transport-https \
    blobfuse2 \
    ca-certificates \
    cmake \
    cpio \
    cron \
    curl \
    file \
    fuse3 \
    gnupg \
    jq \
    libc6 \
    libfuse3-dev \
    lsb-release \
    msodbcsql18 \
    openjdk-17-jdk \
    openssl \
    p7zip-full \
    pkg-config \
    rpm2cpio \
    software-properties-common \
    unixodbc \
    unixodbc-dev \
    unzip \
    vim \
    wget \
    xdg-utils

sudo add-apt-repository ppa:rmescandon/yq -y
apt-get update
apt-get install -y yq

curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Local spark version is dictated by available runtime in Azure Synapse and Fabric:
#
# - https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-version-support#supported-azure-synapse-runtime-releases
#
SPARK_VERSION='3.5.1'
DELTA_VERSION='3.2.0'
SCALA_VERSION='2.12'
MSSQL_DRIVER_VERSION='13.2.1.jre11'

echo "Installing Apache Spark '$SPARK_VERSION' (for local 'spark-submit', identical to Azure Synapse runtime)"
wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz &&
    tar -xvf spark-$SPARK_VERSION-bin-hadoop3.tgz &&
    mkdir -p /opt/spark &&
    mv spark-$SPARK_VERSION-bin-hadoop3/* /opt/spark &&
    rm -rf spark-$SPARK_VERSION-bin-hadoop3.tgz &&
    rm -rf spark-$SPARK_VERSION-bin-hadoop3

echo "Installing Delta Lake '$DELTA_VERSION' for Spark '$SPARK_VERSION'"
mkdir -p /opt/spark/jars

DELTA_CORE_JAR="delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar"
DELTA_STORAGE_JAR="delta-storage-${DELTA_VERSION}.jar"
MSSQL_JAR="mssql-jdbc-${MSSQL_DRIVER_VERSION}.jar"

wget -P /opt/spark/jars "https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/${DELTA_CORE_JAR}"
wget -P /opt/spark/jars "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/${DELTA_STORAGE_JAR}"
wget -P /opt/spark/jars "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/${MSSQL_DRIVER_VERSION}/${MSSQL_JAR}"

LIVY_VERSION='0.9.0-incubating'
LIVY_VERSION_RC='rc1'
SCALA_VERSION='2.12'
LIVY_DOWNLOAD_URL="https://dist.apache.org/repos/dist/dev/incubator/livy/${LIVY_VERSION}-${LIVY_VERSION_RC}"

echo "Installing Apache Livy '$LIVY_VERSION' (Scala $SCALA_VERSION)"
cd /tmp
wget "${LIVY_DOWNLOAD_URL}/apache-livy-${LIVY_VERSION}_${SCALA_VERSION}-bin.zip"
unzip apache-livy-${LIVY_VERSION}_${SCALA_VERSION}-bin.zip
mkdir -p /opt/livy
mv apache-livy-${LIVY_VERSION}_${SCALA_VERSION}-bin/* /opt/livy
rm -rf apache-livy-${LIVY_VERSION}_${SCALA_VERSION}-bin.zip
rm -rf apache-livy-${LIVY_VERSION}_${SCALA_VERSION}-bin

cat > /opt/livy/conf/livy.conf << 'EOF'
# Livy configuration for local dev container with Spark 3.x
livy.spark.master = local[*]
livy.spark.deploy-mode = client
livy.file.local-dir-whitelist = /
livy.server.session.timeout = 1h
livy.repl.enable-hive-context = true
livy.server.spark-home = /opt/spark
EOF

cat > /opt/livy/conf/livy-env.sh << 'EOF'
export SPARK_HOME=/opt/spark
export SPARK_CONF_DIR=/opt/spark/conf
EOF

mkdir -p /opt/livy/logs
chmod 777 /opt/livy/logs

# Allow all users to access FUSE mounts
sed -i 's/^#user_allow_other/user_allow_other/' /etc/fuse.conf

sudo apt-get autoremove -y &&
    sudo apt-get clean -y &&
    sudo rm -rf /var/lib/apt/lists/* &&
    sudo rm -rf /tmp/downloads
