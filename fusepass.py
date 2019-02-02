#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

from collections import defaultdict
from errno import ENOENT
from stat import S_IFMT, S_IMODE, S_IFDIR
from time import time
from os import path
import logging
import pickle

import config

from lib.fusell import FUSELL

from evernote.edam.notestore.ttypes import NoteFilter

EVERNOTE_DATA_FILE = '.evernote_data'
NOTES_LOAD_BATCH_SIZE = 100

NOTE_HEAD_1 = '''<?xml version="1.0" encoding="UTF-8"?>'''
NOTE_HEAD_2 = '''<!DOCTYPE en-note SYSTEM "http://xml.evernote.com/pub/enml2.dtd">'''
NOTE_HEAD_3 = '''<!DOCTYPE en-note SYSTEM 'http://xml.evernote.com/pub/enml2.dtd'>'''

class EvernoteFuse(FUSELL):

    def __init__(self, mount_point, evernote):
        """
        Evernote fuse

        :type evernote: evernote.api.client.EvernoteClient
        """
        self.evernote = evernote

        self.notebooks = {}
        self.notebook_ino = {}
        self.notebooks_sync_time = 0

        self.notebooks_notes_sync_time = {}
        self.notebook_notes = {}
        self.notes_ino = {}
        self.note_sync_time = {}

        self.root_ino = 1
        self.ino = self.root_ino
        self.attr = defaultdict(dict)
        self.data = defaultdict(bytes)
        self.parent = {}
        self.children = defaultdict(dict)

        if path.exists(EVERNOTE_DATA_FILE):
            for k, v in pickle.load(open(EVERNOTE_DATA_FILE, 'rb')).items():
                self.__setattr__(k, v)

        self.note_store = self.evernote.get_note_store()

        super(EvernoteFuse, self).__init__(mount_point)

    def destroy(self, user_data):
        """
        save all data to file here, to do less syncing next time
        """
        pickle.dump({
            'notebooks': self.notebooks,
            'notebooks_sync_time': self.notebooks_sync_time,
            'notebooks_notes_sync_time': self.notebooks_notes_sync_time,
            'notebook_notes': self.notebook_notes,
            'note_sync_time': self.note_sync_time,
            'data': self.data,
        }, open(EVERNOTE_DATA_FILE, 'wb'))

    def should_sync_note(self, note):
        return (note.guid not in self.note_sync_time or
                self.note_sync_time[note.guid] + config.NOTE_SYNC_PERIOD <= time())

    def get_note_ino(self, note_guid):
        for ino, note in self.notes_ino.items():
            if note.guid == note_guid:
                return ino

    def sync_note(self, note):
        if not self.should_sync_note(note):
            return

        logging.info('sync note: ' + note.title)
        ino = self.get_note_ino(note.guid)

        note_content = self.note_store.getNoteContent(note.guid)
        note_content = note_content.strip()
        if note_content.startswith(NOTE_HEAD_1):
            note_content = note_content.replace(NOTE_HEAD_1, '', 1).strip()
        if note_content.startswith(NOTE_HEAD_2):
            note_content = note_content.replace(NOTE_HEAD_2, '', 1).strip()
        if note_content.startswith(NOTE_HEAD_3):
            note_content = note_content.replace(NOTE_HEAD_3, '', 1).strip()
        if not note_content.startswith('<en-note>'):
            raise AssertionError('Note "' + note.title + '" has invalid root tag: ' + note_content)

        note_content = note_content[len('<en-note>'):len(note_content) - len('</en-note>')].strip()

        note_content_bytes = note_content.encode('utf-8')
        self.data[ino] = note_content_bytes
        self.attr[ino]['st_size'] = len(note_content_bytes)

        self.note_sync_time[note.guid] = time()
        logging.info('sync note - done: ' + note.title)


    def should_sync_notebook_notes(self, notebook):
        return (notebook.guid not in self.notebooks_notes_sync_time or
                self.notebooks_notes_sync_time[notebook.guid] + config.NOTEBOOK_NOTES_SYNC_PERIOD <= time())

    def sync_notebook_notes(self, notebook):
        if not self.should_sync_notebook_notes(notebook):
            return

        logging.info('sync notebook: ' + notebook.name)
        note_filter = NoteFilter()
        note_filter.notebookGuid = notebook.guid

        note_list = []
        current_offset = 0
        while True:
            logging.info('sync notebook: ' + notebook.name + ' - ' + str(current_offset))
            note_batch = self.note_store.findNotes(note_filter, current_offset, NOTES_LOAD_BATCH_SIZE + 1)
            if len(note_batch.notes) < NOTES_LOAD_BATCH_SIZE + 1:
                note_list += note_batch.notes
                break
            else:
                note_list += note_batch.notes[:-1]
                current_offset += NOTES_LOAD_BATCH_SIZE

        if notebook.guid in self.notebook_notes:
            prev_notes = self.notebook_notes[notebook.guid].copy()
        else:
            prev_notes = {}
        new_notes = {}

        for note in note_list:
            new_notes[note.guid] = note
            if note.guid not in prev_notes:
                logging.info('sync new note: ' + note.title)
                self.add_notebook_note_to_fuse(note)
            elif note.title != prev_notes[note.guid].title:
                logging.info('sync note renamed: ' + prev_notes[note.guid].title + '->' + note.title)
                self.rename_notebook_note_in_fuse(note.notebookGuid, prev_notes[note.guid].title, note.title)

        for prev_note_guid, prev_note in prev_notes.items():
            if prev_note_guid not in new_notes:
                logging.info('sync: note deleted: ' + prev_note.name)
                self.remove_notebook_note_from_fuse(prev_note.notebookGuid, prev_note_guid)

        self.notebook_notes[notebook.guid] = new_notes
        logging.info('sync notebook - done: ' + notebook.name)
        self.notebooks_notes_sync_time[notebook.guid] = time()

    def should_sync_notebooks(self):
        return self.notebooks_sync_time + config.NOTEBOOK_SYNC_PERIOD <= time()

    def sync_notebooks(self):
        if not self.should_sync_notebooks():
            # it is too early to sync
            return

        logging.info('sync: notebooks')

        prev_notebooks = self.notebooks.copy()

        for notebook in self.note_store.listNotebooks():
            self.notebooks[notebook.guid] = notebook
            if notebook.guid not in prev_notebooks:
                logging.info('sync: new notebook: ' + notebook.name)
                self.add_notebook_to_fuse(notebook.guid)
            elif notebook.name != prev_notebooks[notebook.guid].name:
                logging.info('sync: notebook renamed: ' + prev_notebooks[notebook.guid].name + '->' + notebook.name)
                self.rename_notebook_in_fuse(prev_notebooks[notebook.guid].name, notebook.name)

        for prev_notebook_guid, prev_notebook in prev_notebooks.items():
            if prev_notebook_guid not in self.notebooks:
                logging.info('sync: notebook deleted: ' + prev_notebook.name)
                self.remove_notebook_from_fuse(prev_notebook_guid)

        self.notebooks_sync_time = time()
        logging.info('sync: notebooks - done')

    def remove_notebook_note_from_fuse(self, notebook_guid, note_guid):
        parent = self.notebook_ino[notebook_guid]
        note_name = self.notebook_notes[notebook_guid][note_guid]
        ino = self.children[parent][note_name]

        del self.children[parent][note_name]
        self.attr[parent]['st_nlink'] -= 1
        del self.attr[ino]

        del self.notes_ino[ino]

    def rename_notebook_note_in_fuse(self, notebook_guid, prev_name, new_name):
        parent = self.notebook_ino[notebook_guid]
        self.children[parent][new_name] = self.children[parent][prev_name]
        del self.children[parent][prev_name]

    def add_notebook_note_to_fuse(self, note):
        ino = self.create_ino()
        now = time()
        attr = dict(
            st_ino=ino,
            st_mode=0o100664,
            st_nlink=1,
            ct_rdev=0,
            st_atime=now,
            st_mtime=note.updated,
            st_ctime=note.created,
            st_size=note.contentLength,
        )
        if 'st_uid' in self.attr[self.root_ino]:
            attr['st_uid'] = self.attr[self.root_ino]['st_uid']
        if 'st_gid' in self.attr[self.root_ino]:
            attr['st_gid'] = self.attr[self.root_ino]['st_gid']

        parent = self.notebook_ino[note.notebookGuid]
        self.attr[ino] = attr
        self.attr[parent]['st_nlink'] += 1
        self.children[parent][note.title] = ino
        self.parent[ino] = parent

        self.notes_ino[ino] = note

    def add_notebook_notes_to_fuse(self, notebook_guid):
        notebook_notes = self.notebook_notes[notebook_guid]
        for note in notebook_notes.values():
            self.add_notebook_note_to_fuse(note)

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

    def get_notebook_by_ino(self, ino):
        for notebook_guid, notebook_ino in self.notebook_ino.items():
            if notebook_ino == ino:
                return self.notebooks[notebook_guid]
        raise AssertionError("Notebook with ino not found: " + ino)

    def create_ino(self):
        self.ino += 1
        return self.ino

    def init(self, userdata, conn):
        self.attr[1] = dict(
            st_ino=1,
            st_mode=S_IFDIR | 0o777,
            st_nlink=2)
        self.parent[1] = 1

        for notebook_guid in self.notebooks:
            self.add_notebook_to_fuse(notebook_guid)
            if notebook_guid in self.notebook_notes:
                self.add_notebook_notes_to_fuse(notebook_guid)

        self.sync_notebooks()

        logging.info('init done')

    def getattr(self, req, ino, fi):
        attr = self.attr[ino]
        if attr:
            self.reply_attr(req, attr, 1.0)
        else:
            self.reply_err(req, ENOENT)

    def lookup(self, req, parent, name):
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
        print('mknod:', parent, name, mode, rdev)
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
        self.parent[ino] = parent

        entry = dict(
            ino=ino,
            attr=attr,
            attr_timeout=1.0,
            entry_timeout=1.0)
        self.reply_entry(req, entry)

    def open(self, req, ino, fi):
        print('open:', ino)
        if ino in self.notes_ino:
            self.sync_note(self.notes_ino[ino])
        self.reply_open(req, fi)

    def read(self, req, ino, size, off, fi):
        print('read:', ino, size, off)
        buf = self.data[ino][off:(off + size)]
        self.reply_buf(req, buf)

    def readdir(self, req, ino, size, off, fi):
        parent = self.parent[ino]
        entries = [
            ('.', {'st_ino': ino, 'st_mode': S_IFDIR}),
            ('..', {'st_ino': parent, 'st_mode': S_IFDIR})]

        if ino in self.notebook_ino.values():
            self.sync_notebook_notes(self.get_notebook_by_ino(ino))

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
