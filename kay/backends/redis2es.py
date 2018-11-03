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
                                                   ITEMS_TYPE,
                                                   PERCEVAL,
                                                   ES_TIMEOUT,
                                                   ES_MAX_RETRIES,
                                                   ES_RETRY_ON_TIMEOUT,
                                                   ES_VERIFY_CERTS)

logger = logging.getLogger(__name__)


class Redis2Es(Backend):
    """Backend class to transfer data from a redis queue to an ES index."""

    version = '0.1.0'

    def __init__(self, redis_url, es_url, es_index, es_alias=None,
                 es_items_type=ITEMS_TYPE, es_items_mapping=PERCEVAL,
                 es_timeout=ES_TIMEOUT, es_max_retries=ES_MAX_RETRIES,
                 es_retry_on_timeout=ES_RETRY_ON_TIMEOUT, es_verify_certs=ES_VERIFY_CERTS):

        redis = RedisConnector(redis_url)
        es = ESConnector(es_url, es_index, alias=es_alias,
                         items_type=es_items_type, items_mapping=es_items_mapping,
                         timeout=es_timeout, max_retries=es_max_retries,
                         retry_on_timeout=es_retry_on_timeout, verify_certs=es_verify_certs)

        super().__init__(redis, es)


class Redis2EsCommand(BackendCommand):
    """Class to run Redis2Es backend from the command line."""

    BACKEND = Redis2Es

    @staticmethod
    def setup_cmd_parser():
        """Returns the Redis2Es argument parser."""

        parser = BackendCommandArgumentParser()

        redis = parser.parser.add_argument_group('Redis arguments')
        RedisConnectorCommand.fill_argument_group(redis)

        es = parser.parser.add_argument_group("ES arguments")
        ESConnectorCommand.fill_argument_group(es)

        return parser
