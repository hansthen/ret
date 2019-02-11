FROM ubuntu:18.04
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LANGUAGE en_US.UTF-8
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update -y && apt-get install -y apt-utils locales && locale-gen en_US.UTF-8 && dpkg-reconfigure locales && \
    apt-get install -y tzdata python3 python3-pip python3-dev python3-tk build-essential
COPY data ./data
ADD main.py .
ADD requirements.txt .
RUN sed -i "/pkg-resources==0.0.0/d" requirements.txt
RUN pip3 install -r requirements.txt 
CMD python3 main.py

