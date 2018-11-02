# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2018 Bitergia
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

from kay.backend import (Backend,
                         BackendCommand,
                         BackendCommandArgumentParser)
from kay.backends.connectors.redis import (RedisConnector,
                                           RedisConnectorCommand)
from kay.backends.connectors.elasticsearch import (ESConnector,
                                                   ESConnectorCommand,
                                                   ITEM_TYPE,
                                                   ES_TIMEOUT,
                                                   ES_MAX_RETRIES,
                                                   ES_RETRY_ON_TIMEOUT,
                                                   ES_VERIFY_CERTS)

logger = logging.getLogger(__name__)


class Redis2Es(Backend):
    """Backend class to transfer data from a redis queue to an ES index."""

    version = '0.1.0'

    def __init__(self, redis_url, es_url, es_index, alias=None, item_type=ITEM_TYPE,
                 timeout=ES_TIMEOUT, max_retries=ES_MAX_RETRIES,
                 retry_on_timeout=ES_RETRY_ON_TIMEOUT, verify_certs=ES_VERIFY_CERTS):

        redis = RedisConnector(redis_url)
        es = ESConnector(es_url, es_index, alias=alias, item_type=item_type, timeout=timeout,
                         max_retries=max_retries, retry_on_timeout=retry_on_timeout, verify_certs=verify_certs)

        super().__init__(redis, es)


class Redis2EsCommand(BackendCommand):
    """Class to run Redis2Es backend from the command line."""

    BACKEND = Redis2Es

    @staticmethod
    def setup_cmd_parser():
        """Returns the Redis2Es argument parser."""

        parser = BackendCommandArgumentParser()

        group = parser.parser.add_argument_group('Redis arguments')
        group.add_argument('--redis-url', dest='redis_url', help="Redis URL")

        group = parser.parser.add_argument_group('ES arguments')
        group.add_argument('--index-alias', dest='alias', default='',
                           help="Assign an alias to the index")
        group.add_argument('--items-type', dest='item_type',
                           default=ITEM_TYPE,
                           help="Set the type of items to insert")
        group.add_argument('--es-timeout', dest='timeout',
                           default=ES_TIMEOUT,
                           help="Set timeout")
        group.add_argument('--es-max-retries', dest='max_retries',
                           default=ES_MAX_RETRIES,
                           help="Set max retries")
        group.add_argument('--es-no-retry-on-timeout', dest='retry_on_timeout',
                           default=ES_RETRY_ON_TIMEOUT,
                           action='store_false',
                           help="Disable retries on timeout")
        group.add_argument('--es-verify-certs', dest='verify_certs',
                           default=ES_VERIFY_CERTS,
                           action='store_true',
                           help="Enable verify certs")
        group.add_argument('--es-url', dest='es_url', help="ES url")
        group.add_argument('--es-index', dest='es_index', help="target index")

        return parser
