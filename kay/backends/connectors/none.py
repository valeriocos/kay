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

import logging

from kay.connector import (Connector,
                           ConnectorCommand)

logger = logging.getLogger(__name__)


class NoneConnector(Connector):

    def __init__(self):
        super().__init__("none")

    async def write(self, data_queue):
        """Write data to ElasticSearch"""

        while True:
            item = await data_queue.get()

            if item == Connector.READ_DONE:
                break

        data_queue.task_done()


class NoneConnectorCommand(ConnectorCommand):
    """Class to initialize ESConnector from the command line."""

    @staticmethod
    def fill_argument_group(group):
        """"Fill the ESConnector group parser."""
