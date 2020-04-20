This program provides a real time visual feed of metro lines
in Rotterdam.

Installation
===================
To install the simulator, you need to install docker first and then build
```shell
docker build -t ret .
xhost +local:root
docker run --net=host --env="DISPLAY" --volume="$HOME/.Xauthority:/root/.Xauthority:rw" -i -t ret
```
