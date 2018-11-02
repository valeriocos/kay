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

from kay.connector import Connector

from grimoirelab_toolkit.introspect import find_signature_parameters

logger = logging.getLogger(__name__)


class Messenger:
    """Messenger class to transfer data from a source to a target storage.

    :param source_conn: a Connector object to interact with the source storage
    :param target_conn: a Connector object to interact with the target storage
    """

    version = '0.1.0'

    def __init__(self, source_conn, target_conn):
        self.source_conn = source_conn
        self.target_conn = target_conn

    def transfer(self, keep_alive=True):
        """Transfer the data from the source to the target storages.

        :param keep_alive: a flag to keeps listening to the source storage
        """

        data_queue = asyncio.Queue()
        loop = asyncio.get_event_loop()

        while True:
            try:
                loop.create_task(self.source_conn.read(data_queue))
                loop.run_until_complete(self.target_conn.write(data_queue))

                if not keep_alive:
                    break

            except KeyboardInterrupt:
                if data_queue.qsize() != 0:
                    data_queue.put(Connector.READ_DONE)
                    loop.run_until_complete(self.target_conn.write(data_queue))
                break

        if data_queue.qsize() != 0:
            logger.warning("%s items have been lost before closing the transfer", data_queue.qsize)

        loop.close()


class MessengerCommandArgumentParser:
    """Manage and parse messenger command arguments."""

    def __init__(self):
        self.parser = argparse.ArgumentParser()

    def parse(self, *args):
        """Parse a list of arguments.

        Parse argument strings needed to run a messenger command. The result
        will be a `argparse.Namespace` object populated with the values
        obtained after the validation of the parameters.

        :param args: argument strings

        :result: an object with the parsed values
        """
        parsed_args = self.parser.parse_args(args)

        return parsed_args


class MessengerCommand:
    """Abstract class to run messengers from the command line.

    When the class is initialized, it parses the given arguments using
    the defined argument parser on `setup_cmd_parser` method. Those
    arguments will be stored in the attribute `parsed_args`.

    The arguments will be used to inizialize and run the `Messenger` object
    assigned to this command. The backend used to run the command is stored
    under `MESSENGER` class attributed. Any class derived from this and must
    set its own `Messenger` class.

    Moreover, the method `setup_cmd_parser` must be implemented to exectute
    the backend.
    """
    MESSENGER = None

    def __init__(self, *args):
        parser = self.setup_cmd_parser()
        self.parsed_args = parser.parse(*args)

    def run(self):
        """Execute messenger.

        This method runs the messenger to transfer items from a source
        data storage to a target one.
        """
        messenger_args = vars(self.parsed_args)
        transfer(self.MESSENGER, messenger_args)

    @staticmethod
    def setup_cmd_parser():
        raise NotImplementedError


def transfer(messenger_class, messenger_args):
    """Transfer items from a data storage to another one.

    :param messenger_class: messenger class to transfer items
    :param messenger_args: dict of arguments needed to init the messenger
    """
    init_args = find_signature_parameters(messenger_class.__init__,
                                          messenger_args)

    messenger = messenger_class(**init_args)

    transfer_args = find_signature_parameters(messenger.transfer,
                                              messenger_args)
    messenger.transfer(**transfer_args)


def find_messengers(top_package):
    """Find available messengers.

    Look for the Kay messengers and commands under `top_package`
    and its sub-packages. When `top_package` defines a namespace,
    backends under that same namespace will be found too.

    :param top_package: package storing backends

    :returns: a tuple with two dicts: one with `Messenger` classes and one
        with `MessengerCommand` classes
    """
    candidates = pkgutil.walk_packages(top_package.__path__,
                                       prefix=top_package.__name__ + '.')

    modules = [name for _, name, is_pkg in candidates if not is_pkg]

    return _import_messengers(modules)


def _import_messengers(modules):
    for module in modules:
        importlib.import_module(module)

    mkls = _find_classes(Messenger, modules)
    ckls = _find_classes(MessengerCommand, modules)

    messengers = {name: kls for name, kls in mkls}
    commands = {name: klass for name, klass in ckls}

    return messengers, commands


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
