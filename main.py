import fusepass
import logging

mountpoint = '/mnt/evernote'

def main():
    logging.basicConfig(level=logging.DEBUG)
    passthrough = fusepass.Memory()
    fusepass.FUSE(passthrough, mountpoint, foreground=True, allow_other=True)

if __name__ == '__main__':
    main()