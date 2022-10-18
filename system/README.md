# SYSTEM installation instruction

## Step 1: Download source code
```
$ git submodule update --init --recursive
```

## Step 2: install c++ prerequisites 

We require g++ version 7.5.0, which is installed by default on Ubuntu18.04. Plz upgrade g++ to 7.5 following this [guide](https://linuxhostsupport.com/blog/how-to-install-gcc-on-centos-7/.

On Ubuntu machines: 
```
$ sudo apt-get -y install autoconf automake libtool cmake autoconf-archive build-essential python3-dev
```
If using anaconda, don't need to install python3-dev. 

On Centos machines: 
```
$ sudo yum install autoconf automake libtool cmake autoconf-archive gcc-c++ centos-release-scl
```

* Plz install cmake from source 3.10.2 if the default cmake version is below 3.10.
```
wget https://cmake.org/files/v3.10/cmake-3.10.2.tar.gz
tar -zxvf cmake-3.10.2.tar.gz
cd cmake-3.10.2
chmod +x ./boostrap
sudo ./boostrap --prefix=/usr/local
sudo make
sudo make install
export PATH=/usr/local/bin:$PATH:$HOME/bin
```

* Install boost 1.68 if libboost-all-dev version is below 1.68 and compile error. 
```
$ sudo apt-get update
$ sudo apt-get -y install python-dev autotools-dev libicu-dev libbz2-dev 
$ wget -O boost_1_68_0.tar.gz https://sourceforge.net/projects/boost/files/boost/1.68.0/boost_1_68_0.tar.gz/download
$ tar xzvf boost_1_68_0.tar.gz
$ cd boost_1_68_0/
$ ./bootstrap.sh --prefix=/usr/local
$ user_configFile=`find $PWD -name user-config.jam`
$ echo "using mpi ;" >> $user_configFile
$ n=`cat /proc/cpuinfo | grep "cpu cores" | uniq | awk '{print $NF}'`
$ sudo ./b2 --with=all -j $n install 
$ sudo sh -c 'echo "/usr/local/lib" >> /etc/ld.so.conf.d/local.conf'
$ sudo ldconfig
```

* If it's needed, add the following lines to CMakeLists.txt before finding boost package.
```
# INCLUDE_DIRECTORIES(SYSTEM /usr/include) 

SET (BOOST_ROOT "/usr/local")
SET (BOOST_INCLUDEDIR "/usr/local/include")
SET (BOOST_LIBRARYDIR "/usr/local/lib")

SET (BOOST_MIN_VERSION "1.68.0")
set (Boost_NO_BOOST_CMAKE ON)
```

* build protobuf. In Centos, it occurs a libtool conflict between 2.4.2 (default) and 2.4.6 (conda version) versions. To fix it, let's uninstall the conda version `conda uninstall libtool`, remove protobuf folders and try again.
```
$ cd /path/to/system_folder/thirdparty
$ mkdir protobuf-lib
$ cd protobuf
$ ./autogen.sh
$ ./configure --prefix=/path/to/system_folder/thirdparty/protobuf-lib
$ make && make check
$ make install
$ sudo ldconfig
```

* build tensorboard_logger. Plz add this line to tensorboard_logger CMAKELists.txt set (CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fPIC")
```
$ cd /path/to/system_folder/thirdparty/tensorboard_logger
$ mkdir build & cd build
$ cmake -DCMAKE_PREFIX_PATH=/path/to/system_folder/thirdparty/protobuf-lib ..
$ make -j20
$ mv *.h ../include
```

## Step 3: install python prerequisite
```
$ pip install grpcio grpcio-tools pyfiglet clint click psutil tqdm pyarrow pandas
```


## Step 4: build system.
```
$ cd /path/to/system folder/
$ mkdir build && cd build
$ cmake -DCMAKE_BUILD_TYPE=Debug ..
$ make -j<#cores>  // ex> make -j10
```

## Step 5: install system python lib
```
$ cd /path/to/system_folder
$ pip install --user -e ./python
```
