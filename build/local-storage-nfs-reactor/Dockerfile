FROM rockylinux:8

# nfs-reactor mounts per-PVC LVs into the pod so Ganesha (sidecar) can export
# them. Needs LVM userspace for activation, usual filesystem tools for
# mount(8), and drbdsetup for the v2 HA eligibility probe (DRBD Primary
# check on convertible volumes). The kernel module lives on the host; we
# only need the userspace binary inside the container.
RUN yum install -y --allowerasing \
        https://www.elrepo.org/elrepo-release-8.el8.elrepo.noarch.rpm \
    && yum install -y --allowerasing \
        lvm2 \
        xfsprogs \
        e2fsprogs \
        util-linux \
        ca-certificates \
        drbd90-utils \
    && yum clean all \
    && rm -rf /var/cache/yum

COPY ./_build/local-storage-nfs-reactor /local-storage-nfs-reactor

ENTRYPOINT [ "/local-storage-nfs-reactor" ]
