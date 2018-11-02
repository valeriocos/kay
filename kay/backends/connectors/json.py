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

import json
import logging

from kay.connector import Connector

logger = logging.getLogger(__name__)


class JSONConnector(Connector):
    def __init__(self, path):
        super().__init__(path)

    async def read(self, data_queue):
        """Read data from a generator"""

        with open(self.source, 'r') as f:
            items = json.loads(f.read())

        for item in items:
            await data_queue.put(item)

        await data_queue.put(Connector.READ_DONE)