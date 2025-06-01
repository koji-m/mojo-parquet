# mojo-parquet

A native Mojo library that implements the Apache Parquet file format.

> [!NOTE]
> This project is currently in an experimental stage.

## testing

```shell
pixi run test
```

## development

To generate Thrift code for (de)serializing Parquet metadata in `parquet/gen/` based on the [parquet-format](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift), you need to use the Thrift compiler from [koji-m/thrift](https://github.com/koji-m/thrift/tree/add-mojo).

```shell
# clone this repository
git clone https://github.com/koji-m/mojo-parquet.git

# clone Thrift compiler added support for Mojo
git clone -b add-mojo https://github.com/koji-m/thrift.git

# clone parquet-format for Thrift definition
git clone https://github.com/apache/parquet-format.git

cp parquet-format/src/main/thrift/parquet.thrift thrift/

cd thrift

# build Thrift compiler in Thrift Docker container
docker run -v $(pwd):/thrift/src -it --rm thrift /bin/bash

# in Docker container...
./bootstrap.sh

./configure --without-java --without-kotlin --without-erlang --without-nodejs --without-nodets \
		--without-lua --without-perl --without-php --without-php_extension --without-dart --without-ruby \
		--without-go --without-swift --without-cl --without-haxe --without-netstd --without-d \
		--without-rs --without-python --without-py3
make

sudo make install

thrift -r --gen mojo --out gen parquet.thrift

exit

# copy generated Thrift code
cp -rf gen ../mojo-parquet/parquet/
```

The code for the Thrift library for Mojo is managed in `lib/mojo/thrift/` of [koji-m/thrift](https://github.com/koji-m/thrift/tree/add-mojo).
After updating the code, copy it to `thrift/`.
