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
import urllib3
urllib3.disable_warnings()

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from kay.connector import (Connector,
                           ConnectorCommand)
from kay.errors import ElasticError

ITEM_TYPE = 'items'
ES_BULK_SIZE = 100
ES_TIMEOUT = 3600
ES_MAX_RETRIES = 50
ES_RETRY_ON_TIMEOUT = True
ES_VERIFY_CERTS = False

logger = logging.getLogger(__name__)


class ESConnector(Connector):
    MAPPING_TEMPLATE = """
    { 
      "mappings": { 
        "%s": { 
            "dynamic": false,
            "properties": {
                "backend_name" : {
                    "type" : "keyword"
                },
                "backend_version" : {
                    "type" : "keyword"
                },
                "category" : {
                    "type" : "keyword"
                },
                "data" : {
                    "properties":{}
                },
                "origin" : {
                    "type" : "keyword"
                },
                "perceval_version" : {
                    "type" : "keyword"
                },
                "tag" : {
                    "type" : "keyword"
                },
                "timestamp" : {
                    "type" : "long"
                },
                "updated_on" : {
                    "type" : "long"
                },
                "uuid" : {
                    "type" : "keyword"
                }
            }
        }
      }
    }
    """

    def __init__(self, url, index, alias=None, item_type=ITEM_TYPE,
                 timeout=ES_TIMEOUT, max_retries=ES_MAX_RETRIES,
                 retry_on_timeout=ES_RETRY_ON_TIMEOUT, verify_certs=ES_VERIFY_CERTS):
        super().__init__("elasticsearch")
        self.conn = Elasticsearch([url], timeout=timeout, max_retries=max_retries,
                                  retry_on_timeout=retry_on_timeout, verify_certs=verify_certs)
        self.url = url
        self.index = index
        self.alias = alias
        self.item_type = item_type

        self.create_index()

        if self.alias:
            self.create_alias()

    def create_index(self):
        """Create index if not exists"""

        mapping = json.loads(ESConnector.MAPPING_TEMPLATE % self.item_type)

        if self.conn.indices.exists(index=self.index):
            logger.warning("Index %s already exists!", self.index)
            return

        res = self.conn.indices.create(index=self.index, body=mapping)

        if not res['acknowledged']:
            raise ElasticError(cause="Index not created")

    def create_alias(self):
        """Create alias for index"""

        res = self.conn.indices.update_aliases(
            {
                "actions": [
                    {"add": {"index": self.index, "alias": self.alias}}
                ]
            }
        )

        if not res['acknowledged']:
            raise ElasticError(cause="Index not created")

    async def write(self, data_queue):
        """Write data to ElasticSearch"""

        items = []
        while True:
            item = await data_queue.get()

            if item == Connector.READ_DONE:
                break

            items.append(item)
            data_queue.task_done()

            if len(items) == ES_BULK_SIZE:
                self.__process_items(items)
                items.clear()

        if items:
            self.__process_items(items)
            items.clear()

        data_queue.task_done()

    def __process_items(self, items):
        digest_items = []

        for item in items:
            es_item = {
                '_index': self.index,
                '_type': self.item_type,
                '_id': item['uuid'],
                '_source': item
            }

            digest_items.append(es_item)

        self.__write_to_es(digest_items)

    def __write_to_es(self, items):

        index = self.index

        errors = helpers.bulk(self.conn, items)[1]
        self.conn.indices.refresh(index=index)

        if errors:
            raise ElasticError(cause="Lost items from Arthur to ES (%s). Error %s"
                                     % (self.url, errors[0]))


class ESConnectorCommand(ConnectorCommand):
    """Class to initialize ESConnector from the command line."""

    @staticmethod
    def fill_argument_group(group):
        """"Fill the ESConnector group parser."""

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
