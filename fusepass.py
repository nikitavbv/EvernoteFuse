#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

from collections import defaultdict
from errno import ENOENT
from stat import S_IFMT, S_IMODE, S_IFDIR
from time import time
import logging

from lib.fusell import FUSELL


class EvernoteFuse(FUSELL):

    def __init__(self, mount_point, evernote):
        """
        Evernote fuse

        :type evernote: evernote.api.client.EvernoteClient
        """
        self.evernote = evernote

        self.notebooks = {}
        self.notebook_ino = {}
        self.root_ino = 1

        self.ino = self.root_ino
        self.attr = defaultdict(dict)
        self.data = defaultdict(bytes)
        self.parent = {}
        self.children = defaultdict(dict)

        self.note_store = self.evernote.get_note_store()

        super(EvernoteFuse, self).__init__(mount_point)

    def sync_notebooks(self):
        logging.info('sync: notebooks')

        prev_notebooks = self.notebooks.copy()

        for notebook in self.note_store.listNotebooks():
            self.notebooks[notebook.guid] = notebook
            if notebook.guid not in prev_notebooks:
                logging.info('sync: new notebook: ' + notebook.name)
                self.add_notebook_to_fuse(notebook.guid)
            elif notebook.name != prev_notebooks[notebook.guid].name:
                logging.info('sync: notebook renamed: ' + prev_notebooks[notebook.guid].name + '->' + notebook.name)
                self.rename_notebook_in_fuse(notebook.guid, prev_notebooks[notebook.guid].name, notebook.name)

        for prev_notebook_guid, prev_notebook in prev_notebooks.items():
            logging.info('sync: notebook deleted: ' + prev_notebook.name)
            self.remove_notebook_from_fuse(prev_notebook_guid)

    def rename_notebook_in_fuse(self, prev_name, new_name):
        self.children[self.root_ino][new_name] = self.children[self.root_ino][prev_name]
        del self.children[self.root_ino][prev_name]

    def remove_notebook_from_fuse(self, notebook_guid):
        ino = self.notebook_ino[notebook_guid]
        notebook_name = self.notebooks[notebook_guid].name

        del self.notebook_ino[notebook_guid]
        del self.children[self.root_ino][notebook_name]
        del self.parent[ino]
        self.attr[self.root_ino]['st_nlink'] -= 1
        del self.attr[ino]

    def add_notebook_to_fuse(self, notebook_guid):
        ino = self.create_ino()
        now = time()

        attr = dict(
            st_ino=ino,
            st_mode=S_IFDIR | 0o777,
            st_nlink=2,
            st_atime=now,
            st_mtime=now,
            st_ctime=now
        )
        if 'st_uid' in self.attr[self.root_ino]:
            attr['st_uid'] = self.attr[self.root_ino]['st_uid']
        if 'st_gid' in self.attr[self.root_ino]:
            attr['st_gid'] = self.attr[self.root_ino]['st_gid']

        self.attr[ino] = attr
        self.attr[self.root_ino]['st_nlink'] += 1
        self.parent[ino] = self.root_ino
        self.children[self.root_ino][self.notebooks[notebook_guid].name] = ino

        self.notebook_ino[notebook_guid] = ino

    def create_ino(self):
        self.ino += 1
        return self.ino

    def init(self, userdata, conn):
        self.attr[1] = dict(
            st_ino=1,
            st_mode=S_IFDIR | 0o777,
            st_nlink=2)
        self.parent[1] = 1

        self.sync_notebooks()

    def getattr(self, req, ino, fi):
        print('getattr:', ino)
        attr = self.attr[ino]
        if attr:
            self.reply_attr(req, attr, 1.0)
        else:
            self.reply_err(req, ENOENT)

    def lookup(self, req, parent, name):
        print('lookup:', parent, name)
        print(self.children[self.root_ino])
        children = self.children[parent]
        ino = children.get(name, 0)
        attr = self.attr[ino]

        if attr:
            entry = dict(
                ino=ino,
                attr=attr,
                attr_timeout=1.0,
                entry_timeout=1.0)
            self.reply_entry(req, entry)
        else:
            self.reply_err(req, ENOENT)

    def mkdir(self, req, parent, name, mode):
        print('mkdir:', parent, name, mode)
        # 493 for drwxr-xr-x
        ino = self.create_ino()
        ctx = self.req_ctx(req)
        now = time()
        attr = dict(
            st_ino=ino,
            st_mode=S_IFDIR | mode,
            st_nlink=2,
            st_uid=ctx['uid'],
            st_gid=ctx['gid'],
            st_atime=now,
            st_mtime=now,
            st_ctime=now)

        self.attr[ino] = attr
        self.attr[parent]['st_nlink'] += 1
        self.parent[ino] = parent
        self.children[parent][name] = ino

        entry = dict(
            ino=ino,
            attr=attr,
            attr_timeout=1.0,
            entry_timeout=1.0)
        self.reply_entry(req, entry)

    def mknod(self, req, parent, name, mode, rdev):
        print('mknod:', parent, name)
        ino = self.create_ino()
        ctx = self.req_ctx(req)
        now = time()
        attr = dict(
            st_ino=ino,
            st_mode=mode,
            st_nlink=1,
            st_uid=ctx['uid'],
            st_gid=ctx['gid'],
            st_rdev=rdev,
            st_atime=now,
            st_mtime=now,
            st_ctime=now)

        self.attr[ino] = attr
        self.attr[parent]['st_nlink'] += 1
        self.children[parent][name] = ino

        entry = dict(
            ino=ino,
            attr=attr,
            attr_timeout=1.0,
            entry_timeout=1.0)
        self.reply_entry(req, entry)

    def open(self, req, ino, fi):
        print('open:', ino)
        self.reply_open(req, fi)

    def read(self, req, ino, size, off, fi):
        print('read:', ino, size, off)
        buf = self.data[ino][off:(off + size)]
        self.reply_buf(req, buf)

    def readdir(self, req, ino, size, off, fi):
        print('readdir:', ino)
        parent = self.parent[ino]
        entries = [
            ('.', {'st_ino': ino, 'st_mode': S_IFDIR}),
            ('..', {'st_ino': parent, 'st_mode': S_IFDIR})]
        for name, child in self.children[ino].items():
            entries.append((name, self.attr[child]))
        self.reply_readdir(req, size, off, entries)

    def rename(self, req, parent, name, newparent, newname):
        print('rename:', parent, name, newparent, newname)
        ino = self.children[parent].pop(name)
        self.children[newparent][newname] = ino
        self.parent[ino] = newparent
        self.reply_err(req, 0)

    def setattr(self, req, ino, attr, to_set, fi):
        print('setattr:', ino, to_set)
        a = self.attr[ino]
        for key in to_set:
            if key == 'st_mode':
                # Keep the old file type bit fields
                a['st_mode'] = S_IFMT(a['st_mode']) | S_IMODE(attr['st_mode'])
            else:
                a[key] = attr[key]
        self.attr[ino] = a
        self.reply_attr(req, a, 1.0)

    def write(self, req, ino, buf, off, fi):
        print('write:', ino, off, len(buf))
        self.data[ino] = self.data[ino][:off] + buf
        self.attr[ino]['st_size'] = len(self.data[ino])
        self.reply_write(req, len(buf))

    def rmdir(self, req, parent, name):
        ino = self.children[parent][name]

        del self.children[parent][name]
        del self.parent[ino]
        self.attr[parent]['st_nlink'] -= 1
        del self.attr[ino]

        self.reply_err(req, 0)

    def unlink(self, req, parent, name):
        ino = self.children[parent][name]
        del self.children[parent][name]
        self.attr[parent]['st_nlink'] -= 1
        del self.attr[ino]

        self.reply_err(req, 0)
