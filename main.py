import fusepass
import logging
import os
import subprocess
import getpass

mount_point = '/mnt/evernote'


def main():
    logging.basicConfig(level=logging.DEBUG)
    if not mount_point_exists():
        mount_point_create()
    fusepass.Memory(mount_point)


def mount_point_exists():
    return os.path.isdir(mount_point)


def mount_point_create():
    print('Creating mount point requires root')
    subprocess.call(['sudo', 'mkdir', mount_point])
    subprocess.call(['sudo', 'chown', getpass.getuser(), mount_point])
    print('Mount point created:', mount_point)


if __name__ == '__main__':
    main()
