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
import pickle

import redis

from arthur.common import Q_STORAGE_ITEMS

from kay.connector import (Connector,
                           ConnectorCommand)

logger = logging.getLogger(__name__)


class RedisConnector(Connector):
    def __init__(self, redis_url):
        super().__init__("redis")
        self.url = redis_url
        self.conn = redis.StrictRedis.from_url(redis_url)

    async def read(self, data_queue):
        """Read data from Redis queue"""

        pipe = self.conn.pipeline()
        pipe.lrange(Q_STORAGE_ITEMS, 0, -1)
        pipe.ltrim(Q_STORAGE_ITEMS, 1, 0)
        items = pipe.execute()[0]

        for item in items:
            item = pickle.loads(item)
            await data_queue.put(item)

        await data_queue.put(Connector.READ_DONE)


class RedisConnectorCommand(ConnectorCommand):
    """Class to initialize RedisConnector from the command line."""

    BACKEND = RedisConnector

    @staticmethod
    def fill_argument_group(group):
        """Fill the RedisConnector group argument."""

        group.add_argument('--redis-url', dest='redis_url', help="Redis URL")
