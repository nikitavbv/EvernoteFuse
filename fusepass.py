#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

from collections import defaultdict
from errno import ENOENT
from stat import S_IFMT, S_IMODE, S_IFDIR
from time import time
from os import path
from threading import Timer
import logging
import pickle

import config

from lib.fusell import FUSELL

from evernote.edam.notestore.ttypes import NoteFilter
from evernote.edam.type.ttypes import Note

EVERNOTE_DATA_FILE = '.evernote_data'
NOTES_LOAD_BATCH_SIZE = 100
NOTE_CREATION_DELAY = 10.0  # seconds, to avoid creating notes out of temporary files
NOTE_UPDATE_DELAY = 10.0  # seconds, to avoid updating note too frequently

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

        self.note_creation_timers = {}
        self.note_update_timers = {}
        self.note_rename_timers = {}

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

    def create_note(self, ino):
        note_name = self.find_child_by_parent_and_ino(self.parent[ino], ino)
        logging.info('create note: ' + note_name)

        notebook_guid = self.get_notebook_by_ino(self.parent[ino]).guid

        note = Note()
        note.title = note_name
        note.notebookGuid = notebook_guid
        note.content = self.get_note_content_by_ino(ino)
        created_note = self.note_store.createNote(note)
        self.notes_ino[ino] = created_note
        self.notebook_notes[notebook_guid][created_note.guid] = created_note

    def update_note(self, ino):
        note_name = self.find_child_by_parent_and_ino(self.parent[ino], ino)
        logging.info('update note: ' + note_name)

        notebook_guid = self.get_notebook_by_ino(self.parent[ino]).guid
        note = self.find_note_by_name(notebook_guid, note_name)
        note.content = self.get_note_content_by_ino(ino)
        updated_note = self.note_store.updateNote(note)
        self.notes_ino[ino] = updated_note
        self.notebook_notes[notebook_guid][updated_note.guid] = updated_note

    def rename_note(self, ino):
        note_name = self.find_child_by_parent_and_ino(self.parent[ino], ino)
        logging.info('rename note: ' + note_name)

        notebook_guid = self.get_notebook_by_ino(self.parent[ino]).guid
        note = self.notes_ino[ino]
        note.title = note_name
        updated_note = self.note_store.updateNote(note)
        self.notes_ino[ino] = updated_note
        self.notebook_notes[notebook_guid][updated_note.guid] = updated_note

    def get_note_content_by_ino(self, ino):
        content = self.data[ino].decode('utf8')
        return NOTE_HEAD_1 + NOTE_HEAD_2 + '<en-note>' + content + '</en-note>'

    def find_note_by_name(self, notebook_guid, name):
        for note in self.notebook_notes[notebook_guid].values():
            if note.title == name:
                return note

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
            st_mtime=note.updated or now,
            st_ctime=note.created or now,
            st_size=note.contentLength or 0,
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

    def find_child_by_parent_and_ino(self, parent, ino):
        for child_name, child_ino in self.children[parent].items():
            if child_ino == ino:
                return child_name

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

        if not newname.startswith('.'):
            if ino in self.note_rename_timers:
                self.note_rename_timers[ino].cancel()
            timer = Timer(NOTE_UPDATE_DELAY, self.rename_note, [ino])
            timer.daemon = True
            timer.start()
            self.note_rename_timers[ino] = timer

        self.reply_err(req, 0)

    def setattr(self, req, ino, attr, to_set, fi):
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
        self.data[ino] = self.data[ino][:off] + buf
        self.attr[ino]['st_size'] = len(self.data[ino])

        parent = self.parent[ino]
        note_name = self.find_child_by_parent_and_ino(parent, ino)
        notebook_guid = self.get_notebook_by_ino(parent).guid
        note = self.find_note_by_name(notebook_guid, note_name)
        if not note_name.startswith('.'):
            if note is None:
                if ino in self.note_creation_timers:
                    self.note_creation_timers[ino].cancel()
                timer = Timer(NOTE_CREATION_DELAY, self.create_note, [ino])
                timer.daemon = True
                timer.start()
                self.note_creation_timers[ino] = timer
            else:
                if ino in self.note_update_timers:
                    self.note_update_timers[ino].cancel()
                timer = Timer(NOTE_UPDATE_DELAY, self.update_note, [ino])
                timer.daemon = True
                timer.start()
                self.note_update_timers[ino] = timer

        self.reply_write(req, len(buf))

    def rmdir(self, req, parent, name):
        ino = self.children[parent][name]

        del self.children[parent][name]
        del self.parent[ino]
        self.attr[parent]['st_nlink'] -= 1
        del self.attr[ino]

        self.reply_err(req, 0)

    def unlink(self, req, parent, name):
        print('unlink', name)

        ino = self.children[parent][name]

        if ino in self.note_creation_timers:
            self.note_creation_timers[ino].cancel()
            del self.note_creation_timers[ino]
        if ino in self.note_update_timers:
            self.note_update_timers[ino].cancel()
            del self.note_update_timers[ino]
        if ino in self.note_rename_timers:
            self.note_rename_timers[ino].cancel()
            del self.note_rename_timers[ino]

        del self.children[parent][name]
        self.attr[parent]['st_nlink'] -= 1
        del self.attr[ino]

        self.reply_err(req, 0)
