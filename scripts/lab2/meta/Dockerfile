FROM ubuntu:22.04

# Install Ubuntu packages
ARG DEBIAN_FRONTEND=noninteractive
RUN sed -i 's/ports.ubuntu.com/mirror.sjtu.edu.cn/g' /etc/apt/sources.list
RUN apt-get -y update && \
    apt-get -y install \
      build-essential \
      clang-14 \
      clang-format-14 \
      clang-tidy-14 \
      cmake \
      fuse libfuse-dev \
      g++-12 \
      pkg-config \
      zlib1g-dev && \
    update-alternatives --install /usr/bin/cc cc /usr/bin/clang-14 100 && \
    update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-14 100 && \
    ln -s /usr/bin/clang-format-14 /usr/bin/clang-format

# Set the working directory
WORKDIR /home/bin

# Setup user
COPY mypasswd /tmp
RUN useradd --no-log-init -r -m -g sudo stu
RUN cat /tmp/mypasswd | chpasswd
USER stu

# Run the binary
CMD ["./meta"]