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

from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          datetime_to_str)
from kay.connector import (Connector,
                           ConnectorCommand)
from kay.errors import ElasticError

ES_BULK_SIZE = 100
ES_TIMEOUT = 3600
ES_MAX_RETRIES = 50
ES_RETRY_ON_TIMEOUT = True
ES_VERIFY_CERTS = False

logger = logging.getLogger(__name__)

DEFAULT_MAPPING = """
    { 
      "mappings": { 
        "%s": { 
            "dynamic": false,
            "properties": {}
        }
      }
    }
    """

PERCEVAL_MAPPING = """
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

GALAHAD_MAPPING = """
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
                "galahad_version" : {
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
                },
                "perceval_uuid" : {
                    "type" : "keyword"
                }
            }
        }
      }
    }
    """

GRAAL_MAPPING = """
    { 
      "mappings": { 
        "items": { 
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
                "graal_version" : {
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

PERCEVAL_TYPE = 'perceval'
GALAHAD_TYPE = 'galahad'
GRAAL_TYPE = 'graal'

SUPPORTED_MAPPINGS = {
    PERCEVAL_TYPE: PERCEVAL_MAPPING,
    GALAHAD_TYPE: GALAHAD_MAPPING,
    GRAAL_TYPE: GRAAL_MAPPING
}

ALIAS_RAW = 'raw-items'
ALIAS_ENRICH = 'enrich-items'


class ESConnector(Connector):

    def __init__(self, es_url, es_items_type, es_index=None, es_index_alias=None,
                 es_timeout=ES_TIMEOUT, es_max_retries=ES_MAX_RETRIES,
                 es_retry_on_timeout=ES_RETRY_ON_TIMEOUT, es_verify_certs=ES_VERIFY_CERTS):
        super().__init__("elasticsearch")
        self.conn = Elasticsearch([es_url], timeout=es_timeout, max_retries=es_max_retries,
                                  retry_on_timeout=es_retry_on_timeout, verify_certs=es_verify_certs)
        self.url = es_url
        self.items_type = es_items_type
        self.index = es_index if es_index else self.get_index(es_items_type)
        self.alias = es_index_alias if es_index_alias else self.get_alias(es_items_type)

        if es_items_type not in SUPPORTED_MAPPINGS.keys():
            logger.warning("Items mapping %s unknown, setting default mapping", es_items_type)

        self.items_mapping = SUPPORTED_MAPPINGS[es_items_type]

        self.create_index()
        self.create_alias()

    def create_index(self):
        """Create index if not exists"""

        mapping = json.loads(self.items_mapping % self.items_type)

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
            raise ElasticError(cause="Alias not created")

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

    @staticmethod
    def get_index(items_type):
        current_time = datetime_utcnow()
        str_time = datetime_to_str(current_time, '%Y%m%d%H%M%S')

        return ESConnector.get_alias(items_type) + '_' + str_time

    @staticmethod
    def get_alias(items_type):
        if items_type in [PERCEVAL_TYPE, GRAAL_TYPE]:
            return ALIAS_RAW
        else:
            return ALIAS_ENRICH

    def __process_items(self, items):
        digest_items = []

        for item in items:
            es_item = {
                '_index': self.index,
                '_type': self.items_type,
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

        group.add_argument('--es-index', dest='es_index', default='',
                           help="Target index")
        group.add_argument('--es-index-alias', dest='es_index_alias', default='',
                           help="Assign an alias to the index")
        group.add_argument('--es-timeout', dest='es_timeout',
                           default=ES_TIMEOUT,
                           help="Set timeout")
        group.add_argument('--es-max-retries', dest='es_max_retries',
                           default=ES_MAX_RETRIES,
                           help="Set max retries")
        group.add_argument('--es-no-retry-on-timeout', dest='es_retry_on_timeout',
                           default=ES_RETRY_ON_TIMEOUT,
                           action='store_false',
                           help="Disable retries on timeout")
        group.add_argument('--es-verify-certs', dest='es_verify_certs',
                           default=ES_VERIFY_CERTS,
                           action='store_true',
                           help="Enable verify certs")
        group.add_argument('--es-url', dest='es_url', help="ES url")
        group.add_argument('--es-items-type', dest='es_items_type',
                           choices=[PERCEVAL_TYPE, GRAAL_TYPE, GALAHAD_TYPE],
                           help="Set the type of items to insert")
