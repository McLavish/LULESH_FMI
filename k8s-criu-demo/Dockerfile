# LULESH CRIU node-evacuation demo image.
#
# Ships LULESH built against the FMI MPI shim (lulesh2.0), the FMI rank agent
# (fmi-rank-agent), a robust tcpunchd, and criu, so one image serves every role in the
# demo: the per-machine pods (run LULESH ranks + are exec'd into to drive
# `evacuate-local`), the lulesh-restore Knative Service (restore_server.py +
# `restore-remote`), the tcpunchd rendezvous, and the orchestrator Job (which also runs
# the golden reference run). One image for dump AND restore side is not just
# convenience — criu restore re-opens the worker's executable and shared libraries by
# path, so both sides must be byte-identical. CRIU forces the Direct data plane, hence
# tcpunchd ships here too.
#
# Build context is the LULESH repo root (submodules must be initialized):
#   docker build -f k8s-criu-demo/Dockerfile -t lulesh-criu-evac:dev .
#
# Single stage on Ubuntu: criu restore reliability depends on matching the build and
# run userspace, and keeping it one image avoids any ABI/shared-lib mismatch between
# stages.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Build deps (gcc/cmake/boost/hiredis, zlib — FMI links ZLIB), criu build deps, and
# runtime deps (python control libs, envsubst, net tools used by the runbook
# preflights). Kept in one layer so the produced binaries always find their shared
# libraries at runtime. criu is NOT packaged for Ubuntu 24.04 and the host/cluster
# kernels are recent, so it is built from source (v4.2) below.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake git patch ca-certificates \
        libboost-all-dev libhiredis-dev zlib1g-dev \
        libprotobuf-dev libprotobuf-c-dev protobuf-c-compiler protobuf-compiler python3-protobuf \
        libnl-3-dev libnet-dev libcap-dev libbsd-dev libnftables-dev uuid-dev pkg-config \
        python3 python3-redis python3-kubernetes \
        gettext-base iproute2 iputils-ping netcat-openbsd procps redis-tools \
    && rm -rf /var/lib/apt/lists/*

# Build criu from source (pinned to v4.2, matching the version the demo was verified against).
RUN git clone --depth 1 --branch v4.2 https://github.com/checkpoint-restore/criu.git /criu-src \
    && make -C /criu-src -j"$(nproc)" \
    && install -D -m0755 /criu-src/criu/criu /usr/local/sbin/criu \
    && rm -rf /criu-src \
    && criu --version

# Build LULESH + FMI (the top-level CMake forces FMI's CRIU/Redis/tools flags when
# WITH_FMI_CRIU=ON) and tcpunchd from the vendored TCPunch.
COPY . /src
WORKDIR /src
# Apply the tcpunchd robustness patch (lives in the FMI submodule's runbook; reused in
# place). `--forward` exits non-zero when the tree copied into the image already has it
# applied, so accept that case by verifying the patched marker is actually present — but
# fail the build on any other patch failure (e.g. a submodule bump that makes the patch
# stop applying would otherwise silently ship an unpatched, SIGPIPE-crashing tcpunchd).
RUN { patch -p1 -d /src/extern/fmi/extern/TCPunch --forward \
        < /src/extern/fmi/runbooks/k8s-criu-node-evacuation/patches/0001-tcpunchd-robustness.patch \
      || grep -q "SIG_IGN" /src/extern/fmi/extern/TCPunch/server/hole_punching_server.cpp; } \
    && grep -q "getaddrinfo" /src/extern/fmi/extern/TCPunch/client/tcpunch.cpp \
    && cmake -S /src -B /src/build-img -DWITH_FMI_CRIU=ON -DWITH_OPENMP=OFF \
    && cmake --build /src/build-img -j"$(nproc)" \
    && cmake -S /src/extern/fmi/extern/TCPunch/server -B /src/extern/fmi/extern/TCPunch/server/build-img \
    && cmake --build /src/extern/fmi/extern/TCPunch/server/build-img -j"$(nproc)"

# Stage the binaries and demo assets, then drop the source/build tree.
RUN mkdir -p /opt/fmi/bin /opt/fmi/app \
    && cp /src/build-img/lulesh2.0 /opt/fmi/bin/ \
    && cp /src/build-img/extern/fmi/tools/fmi-rank-agent /opt/fmi/bin/ \
    && cp /src/extern/fmi/extern/TCPunch/server/build-img/tcpunchd /opt/fmi/bin/ \
    && cp /src/k8s-criu-demo/fmi-lulesh.json.tmpl /opt/fmi/app/ \
    && cp /src/k8s-criu-demo/fmi-lulesh-golden.json.tmpl /opt/fmi/app/ \
    && cp /src/k8s-criu-demo/supervisor.py /opt/fmi/app/ \
    && cp /src/k8s-criu-demo/restore_server.py /opt/fmi/app/ \
    && cp /src/k8s-criu-demo/golden_run.py /opt/fmi/app/ \
    && cp /src/k8s-criu-demo/orchestrator.py /opt/fmi/app/ \
    && rm -rf /src

ENV PATH=/opt/fmi/bin:${PATH}
ENV FMI_LULESH_BIN=/opt/fmi/bin/lulesh2.0
ENV FMI_AGENT_BIN=/opt/fmi/bin/fmi-rank-agent
WORKDIR /opt/fmi/app

# Default entrypoint is the per-machine supervisor; the restore Service, orchestrator
# Job, and tcpunchd Deployment override the command.
CMD ["python3", "-u", "/opt/fmi/app/supervisor.py"]
