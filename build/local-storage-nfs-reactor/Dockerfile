FROM rockylinux:8

# nfs-reactor mounts per-PVC LVs into the pod so Ganesha (sidecar) can export
# them. It needs the LVM userspace for activation and the usual filesystem
# tools for mount(8). util-linux ships nsenter/losetup/findmnt.
RUN yum install -y --allowerasing \
        lvm2 \
        xfsprogs \
        e2fsprogs \
        util-linux \
        ca-certificates \
    && yum clean all \
    && rm -rf /var/cache/yum

COPY ./_build/local-storage-nfs-reactor /local-storage-nfs-reactor

ENTRYPOINT [ "/local-storage-nfs-reactor" ]
