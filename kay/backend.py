# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2016 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#     Valerio Cosentino <valcos@bitergia.com>

import argparse
import asyncio
import importlib
import logging
import pkgutil
import time

from kay.connector import Connector

from grimoirelab_toolkit.introspect import find_signature_parameters

logger = logging.getLogger(__name__)


KEEP_ALIVE = True
REST_TIME = 0


class Backend:
    """Backend class to transfer data from a source to a target storage.

    :param source_conn: a Connector object to interact with the source storage
    :param target_conn: a Connector object to interact with the target storage
    """

    version = '0.1.0'

    def __init__(self, source_conn, target_conn):
        self.source_conn = source_conn
        self.target_conn = target_conn

    def transfer(self, keep_alive=KEEP_ALIVE, rest=REST_TIME):
        """Transfer the data from the source to the target storages.

        :param keep_alive: a flag to keeps listening to the source storage
        :param rest: the number of seconds to sleep between queue listenings
        """

        data_queue = asyncio.Queue()
        loop = asyncio.get_event_loop()

        while True:
            try:
                loop.create_task(self.source_conn.read(data_queue))
                loop.run_until_complete(self.target_conn.write(data_queue))

                if not keep_alive:
                    break

                if rest:
                    time.sleep(rest)

            except KeyboardInterrupt:
                if data_queue.qsize() != 0:
                    data_queue.put(Connector.READ_DONE)
                    loop.run_until_complete(self.target_conn.write(data_queue))
                break

        if data_queue.qsize() != 0:
            logger.warning("%s items have been lost before closing the transfer", data_queue.qsize)

        loop.close()


class BackendCommandArgumentParser:
    """Manage and parse backend command arguments."""

    def __init__(self):
        self.parser = argparse.ArgumentParser()

        group = self.parser.add_argument_group('general arguments')
        group.add_argument('--keep-alive', dest='keep_alive',
                           action='store_false',
                           help="Disable continue listening of the source storage")
        group.add_argument('--rest', dest='rest',
                           type=int, default=REST_TIME,
                           help="Rest time between queue listenings")

    def parse(self, *args):
        """Parse a list of arguments.

        Parse argument strings needed to run a backend command. The result
        will be a `argparse.Namespace` object populated with the values
        obtained after the validation of the parameters.

        :param args: argument strings

        :result: an object with the parsed values
        """
        parsed_args = self.parser.parse_args(args)

        if not parsed_args.keep_alive and (parsed_args.rest > 0):
            raise AttributeError("no keep-alive and rest > 0 parameters are incompatible")

        return parsed_args


class BackendCommand:
    """Abstract class to run backends from the command line.

    When the class is initialized, it parses the given arguments using
    the defined argument parser on `setup_cmd_parser` method. Those
    arguments will be stored in the attribute `parsed_args`.

    The arguments will be used to inizialize and run the `Backend` object
    assigned to this command. The backend used to run the command is stored
    under `BACKEND` class attributed. Any class derived from this and must
    set its own `Backend` class.

    Moreover, the method `setup_cmd_parser` must be implemented to exectute
    the backend.
    """
    BACKEND = None

    def __init__(self, *args):
        parser = self.setup_cmd_parser()
        self.parsed_args = parser.parse(*args)

    def run(self):
        """Execute backend.

        This method runs the backend to transfer items from a source
        data storage to a target one.
        """
        backend_args = vars(self.parsed_args)
        transfer(self.BACKEND, backend_args)

    @staticmethod
    def setup_cmd_parser():
        raise NotImplementedError


def transfer(backend_class, backend_args):
    """Transfer items from a data storage to another one.

    :param backend_class: backend class to transfer items
    :param backend_args: dict of arguments needed to init the backend
    """
    init_args = find_signature_parameters(backend_class.__init__,
                                          backend_args)

    backend = backend_class(**init_args)

    transfer_args = find_signature_parameters(backend.transfer,
                                              backend_args)
    backend.transfer(**transfer_args)


def find_backends(top_package):
    """Find available backends.

    Look for the Kay backends and commands under `top_package`
    and its sub-packages. When `top_package` defines a namespace,
    backends under that same namespace will be found too.

    :param top_package: package storing backends

    :returns: a tuple with two dicts: one with `Backend` classes and one
        with `BackendCommand` classes
    """
    candidates = pkgutil.walk_packages(top_package.__path__,
                                       prefix=top_package.__name__ + '.')

    modules = [name for _, name, is_pkg in candidates if not is_pkg]

    return _import_backends(modules)


def _import_backends(modules):
    for module in modules:
        importlib.import_module(module)

    bkls = _find_classes(Backend, modules)
    ckls = _find_classes(BackendCommand, modules)

    backends = {name: kls for name, kls in bkls}
    commands = {name: klass for name, klass in ckls}

    return backends, commands


def _find_classes(parent, modules):
    parents = parent.__subclasses__()

    while parents:
        kls = parents.pop()

        m = kls.__module__

        if m not in modules:
            continue

        name = m.split('.')[-1]
        parents.extend(kls.__subclasses__())

        yield name, kls
