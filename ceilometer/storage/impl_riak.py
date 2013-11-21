# -*- encoding: utf-8 -*-
#
# Copyright © 2013 Rackspace
#
# Author: Andrew Melton <andrew.melton@rackspace.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""Riak storage backend
"""

import math

import riak

from oslo.config import cfg

from ceilometer.openstack.common import log
from ceilometer.openstack.common import network_utils
from ceilometer.openstack.common import timeutils
from ceilometer.storage import base
from ceilometer.storage import models

cfg.CONF.import_opt('float_precision', 'ceilometer.storage',
                    group="database")

LOG = log.getLogger(__name__)


class RiakStorage(base.StorageEngine):

    def get_connection(self, conf):
        """Return a Connection instance based on the configuration settings.
        """
        return Connection(conf)


class Connection(base.Connection):
    """Riak connection.
    """

    def __init__(self, conf):
        """Constructor."""

        opts = self._parse_connection_url(conf.database.connection)
        self.client = riak.RiakClient(**opts)
        self.events = riak.RiakBucket(self.client, 'events')

    @staticmethod
    def _parse_connection_url(url):
        """Parse connection parameters from a database url."""

        opts = {}
        result = network_utils.urlsplit(url)
        opts['protocol'] = result.scheme[len('riak+'):]
        if ':' in result.netloc:
            opts['host'], port = result.netloc.split(':')
        else:
            opts['host'] = result.netloc
            port = 8098
        opts['http_port'] = port and int(port) or 8098
        return opts

    def upgrade(self):
        """Migrate the database to `version` or the most recent version."""

    def record_metering_data(self, data):
        """Write the data to the backend storage system.

        :param data: a dictionary such as returned by
                     ceilometer.meter.meter_message_from_counter

        All timestamps must be naive utc datetime object.
        """
        raise NotImplementedError()

    def clear_expired_metering_data(self, ttl):
        """Clear expired data from the backend storage system according to the
        time-to-live.

        :param ttl: Number of seconds to keep records for.

        """
        raise NotImplementedError()

    def get_users(self, source=None):
        """Return an iterable of user id strings.

        :param source: Optional source filter.
        """
        raise NotImplementedError()

    def get_projects(self, source=None):
        """Return an iterable of project id strings.

        :param source: Optional source filter.
        """
        raise NotImplementedError()

    def get_resources(self, user=None, project=None, source=None,
                      start_timestamp=None, start_timestamp_op=None,
                      end_timestamp=None, end_timestamp_op=None,
                      metaquery={}, resource=None, pagination=None):
        """Return an iterable of models.Resource instances containing
        resource information.

        :param user: Optional ID for user that owns the resource.
        :param project: Optional ID for project that owns the resource.
        :param source: Optional source filter.
        :param start_timestamp: Optional modified timestamp start range.
        :param start_timestamp_op: Optional timestamp start range operation.
        :param end_timestamp: Optional modified timestamp end range.
        :param end_timestamp_op: Optional timestamp end range operation.
        :param metaquery: Optional dict with metadata to match on.
        :param resource: Optional resource filter.
        :param pagination: Optional pagination query.
        """
        raise NotImplementedError()

    def get_meters(self, user=None, project=None, resource=None, source=None,
                   metaquery={}, pagination=None):
        """Return an iterable of model.Meter instances containing meter
        information.

        :param user: Optional ID for user that owns the resource.
        :param project: Optional ID for project that owns the resource.
        :param resource: Optional resource filter.
        :param source: Optional source filter.
        :param metaquery: Optional dict with metadata to match on.
        :param pagination: Optional pagination query.
        """
        raise NotImplementedError()

    def get_samples(self, sample_filter, limit=None):
        """Return an iterable of model.Sample instances.

        :param sample_filter: Filter.
        :param limit: Maximum number of results to return.
        """
        raise NotImplementedError()

    def get_meter_statistics(self, sample_filter, period=None, groupby=None):
        """Return an iterable of model.Statistics instances.

        The filter must have a meter value set.
        """
        raise NotImplementedError()

    def get_alarms(self, name=None, user=None,
                   project=None, enabled=None, alarm_id=None, pagination=None):
        """Yields a lists of alarms that match filters
        """
        raise NotImplementedError()

    def create_alarm(self, alarm):
        """Create an alarm. Returns the alarm as created.

        :param alarm: The alarm to create.
        """
        raise NotImplementedError()

    def update_alarm(self, alarm):
        """update alarm
        """
        raise NotImplementedError()

    def delete_alarm(self, alarm_id):
        """Delete a alarm
        """
        raise NotImplementedError()

    def get_alarm_changes(self, alarm_id, on_behalf_of,
                          user=None, project=None, type=None,
                          start_timestamp=None, start_timestamp_op=None,
                          end_timestamp=None, end_timestamp_op=None):
        """Yields list of AlarmChanges describing alarm history

        Changes are always sorted in reverse order of occurence, given
        the importance of currency.

        Segregation for non-administrative users is done on the basis
        of the on_behalf_of parameter. This allows such users to have
        visibility on both the changes initiated by themselves directly
        (generally creation, rule changes, or deletion) and also on those
        changes initiated on their behalf by the alarming service (state
        transitions after alarm thresholds are crossed).

        :param alarm_id: ID of alarm to return changes for
        :param on_behalf_of: ID of tenant to scope changes query (None for
                             administrative user, indicating all projects)
        :param user: Optional ID of user to return changes for
        :param project: Optional ID of project to return changes for
        :project type: Optional change type
        :param start_timestamp: Optional modified timestamp start range
        :param start_timestamp_op: Optional timestamp start range operation
        :param end_timestamp: Optional modified timestamp end range
        :param end_timestamp_op: Optional timestamp end range operation
        """
        raise NotImplementedError()

    def record_alarm_change(self, alarm_change):
        """Record alarm change event.
        """
        raise NotImplementedError()

    def clear(self):
        """Clear database."""
        raise NotImplementedError()

    @staticmethod
    def _to_trait_dict(model):
        data = dict(name=model.name, dtype=model.dtype)

        if model.dtype == models.Trait.DATETIME_TYPE:
            data['value'] = timeutils.isotime(model.value, subsecond=True)
        elif model.dtype == models.Trait.FLOAT_TYPE:
            precision = cfg.CONF.database.float_precision
            data['value'] = int(model.value * math.pow(10, precision))
        else:
            data['value'] = model.value

        return data

    @staticmethod
    def _index_type(dtype):
        if dtype in [models.Trait.INT_TYPE, models.Trait.FLOAT_TYPE]:
            return 'int'
        else:
            return 'bin'

    @classmethod
    def _index_key(cls, trait):
        index_type = cls._index_type(trait['dtype'])
        return 'trait_%s_%s' % (trait['name'], index_type)

    def record_events(self, events):
        """Write the events to the backend storage system.

        :param events: a list of model.Event objects.
        """
        problem_events = []
        for event in events:
            try:
                event_object = self.events.get(key=event.message_id)
                if not event_object.exists:
                    traits = [self._to_trait_dict(t) for t in event.traits]
                    data = {
                        'message_id': event.message_id,
                        'event_name': event.event_name,
                        'generated': timeutils.isotime(event.generated,
                                                       subsecond=True),
                        'traits': traits
                    }
                    event_object.data = data
                    event_object.add_index('event_name_bin',
                                           data['event_name'])
                    event_object.add_index('generated_bin',
                                           data['generated'])
                    for t in traits:
                        event_object.add_index(self._index_key(t), t['value'])
                    event_object.store()
                else:
                    problem_events.append((models.Event.DUPLICATE, event))
            except Exception, e:
                LOG.exception('Failed to record event: %s', e)
                problem_events.append((models.Event.UNKNOWN_PROBLEM,
                                       event))
        return problem_events

    @classmethod
    def _trait_query(cls, trait):
        dtype = None
        value = None
        for key in trait:
            if key != 'key':
                if key == 't_string':
                    dtype = models.Trait.TEXT_TYPE
                    value = trait[key]
                elif key == 't_int':
                    dtype = models.Trait.INT_TYPE
                    value = trait[key]
                elif key == 't_float':
                    dtype = models.Trait.FLOAT_TYPE
                    precision = cfg.CONF.database.float_precision
                    value = int(trait[key] * math.pow(10, precision))
                elif key == 't_datetime':
                    dtype = models.Trait.DATETIME_TYPE
                    value = timeutils.isotime(trait[key], subsecond=True)

        if dtype is None or value is None or trait.get('key') is None:
            raise NotImplementedError()

        index_key = 'trait_%s_%s' % (trait['key'], cls._index_type(dtype))
        return index_key, value

    @staticmethod
    def _to_trait_model(trait_dict):
        dtype = trait_dict['dtype']
        if dtype == models.Trait.DATETIME_TYPE:
            value = timeutils.parse_isotime(trait_dict['value'])
        elif dtype == models.Trait.FLOAT_TYPE:
            precision = cfg.CONF.database.float_precision
            value = int(trait_dict['value'] / math.pow(10, precision))
        else:
            value = trait_dict['value']
        return models.Trait(trait_dict['name'], dtype, value)

    @classmethod
    def _to_event_model(cls, event_dict):
        generated = timeutils.parse_isotime(event_dict['generated'])
        traits = [cls._to_trait_model(t) for t in event_dict['traits']]
        event = models.Event(event_dict['message_id'],
                             event_dict['event_name'],
                             generated,
                             traits)
        return event

    def get_events(self, event_filter):
        """Return an iterable of model.Event objects.
        """
        map_reduce = riak.RiakMapReduce(self.client)

        if event_filter.start and event_filter.end:
            startkey = timeutils.isotime(event_filter.start)
            endkey = None
            if event_filter.end:
                endkey = timeutils.isotime(event_filter.end)
            map_reduce = map_reduce.index('events', 'generated_bin',
                                          startkey,
                                          endkey=endkey)
        if event_filter.event_name:
            map_reduce = map_reduce.index('events', 'event_name_bin',
                                          startkey=event_filter.event_name)
        if event_filter.traits:
            for trait in event_filter.traits:
                index_key, value = self._trait_query(trait)
                map_reduce = map_reduce.index('events', index_key, value)

        map_reduce = map_reduce.map_values_json(options=dict(keep=True))
        results = map_reduce.run()
        events = [self._to_event_model(e) for e in results]
        return events
