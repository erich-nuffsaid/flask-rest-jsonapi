# -*- coding: utf-8 -*-

"""Helper to deal with querystring parameters according to jsonapi specification"""

import json
import re

from flask import current_app
from marshmallow import class_registry

from flask_rest_jsonapi.exceptions import BadRequest, InvalidFilters, InvalidSort, InvalidField, InvalidInclude
from flask_rest_jsonapi.schema import get_model_field, get_related_schema, get_relationships, get_schema_from_type


class QueryStringManager(object):
    """Querystring parser according to jsonapi reference"""

    MANAGED_KEYS = (
        'filter',
        'page',
        'fields',
        'sort',
        'include',
        'q'
    )

    def __init__(self, querystring, schema):
        """Initialization instance

        :param dict querystring: query string dict from request.args
        """
        if not isinstance(querystring, dict):
            raise ValueError('QueryStringManager require a dict-like object querystring parameter')

        self.qs = querystring
        self.schema = schema

    def _get_key_values(self, name):
        """Return a dict containing key / values items for a given key, used for items like filters, page, etc.

        :param str name: name of the querystring parameter
        :return dict: a dict of key / values items
        """
        results = {}

        for key, value in self.qs.items():
            try:
                if not key.startswith(name):
                    continue

                key_start = key.index('[') + 1
                key_end = key.index(']')
                item_key = key[key_start:key_end]

                if ',' in value:
                    item_value = value.split(',')
                else:
                    item_value = value
                results.update({item_key: item_value})
            except Exception:
                raise BadRequest("Parse error", source={'parameter': key})

        return results

    def _simple_filters(self, dict_):
        return [{"name": key, "op": "eq", "val": value}
                for (key, value) in dict_.items()]

    @property
    def querystring(self):
        """Return original querystring but containing only managed keys

        :return dict: dict of managed querystring parameter
        """
        return {key: value for (key, value) in self.qs.items()
                if key.startswith(self.MANAGED_KEYS) or self._get_key_values('filter[')}

    @property
    def filters(self):
        """Return filters from query string.

        :return list: filter information
        """
        results = []
        filters = self.qs.get('filter')
        if filters is not None:
            try:
                results.extend(json.loads(filters))
            except (ValueError, TypeError):
                raise InvalidFilters("Parse error")
        if self._get_key_values('filter['):
            results.extend(self._simple_filters(self._get_key_values('filter[')))
        return results

    @property
    def pagination(self):
        """Return all page parameters as a dict.

        :return dict: a dict of pagination information

        To allow multiples strategies, all parameters starting with `page` will be included. e.g::

            {
                "number": '25',
                "size": '150',
            }

        Example with number strategy::

            >>> query_string = {'page[number]': '25', 'page[size]': '10'}
            >>> parsed_query.pagination
            {'number': '25', 'size': '10'}
        """
        # check values type
        result = self._get_key_values('page')
        for key, value in result.items():
            if key not in ('number', 'size'):
                raise BadRequest("{} is not a valid parameter of pagination".format(key), source={'parameter': 'page'})
            try:
                int(value)
            except ValueError:
                raise BadRequest("Parse error", source={'parameter': 'page[{}]'.format(key)})

        if current_app.config.get('ALLOW_DISABLE_PAGINATION', True) is False and int(result.get('size', 1)) == 0:
            raise BadRequest("You are not allowed to disable pagination", source={'parameter': 'page[size]'})

        if current_app.config.get('MAX_PAGE_SIZE') is not None and 'size' in result:
            if int(result['size']) > current_app.config['MAX_PAGE_SIZE']:
                raise BadRequest("Maximum page size is {}".format(current_app.config['MAX_PAGE_SIZE']),
                                 source={'parameter': 'page[size]'})

        return result

    @property
    def fields(self):
        """Return fields wanted by client.

        :return dict: a dict of sparse fieldsets information

        Return value will be a dict containing all fields by resource, for example::

            {
                "user": ['name', 'email'],
            }

        """
        result = self._get_key_values('fields')
        for key, value in result.items():
            if not isinstance(value, list):
                result[key] = [value]

        for key, value in result.items():
            schema = get_schema_from_type(key)
            for obj in value:
                if obj not in schema._declared_fields:
                    raise InvalidField("{} has no attribute {}".format(schema.__name__, obj))

        return result

    @property
    def sorting(self):
        """Return fields to sort by including sort name for SQLAlchemy and row
        sort parameter for other ORMs

        :return list: a list of sorting information

        Example of return value::

            [
                {'field': 'created_at', 'order': 'desc', 'nulls': None, 'joins': ['car', 'wheel']},
            ]

        """
        if not self.qs.get('sort'):
            return []
        sorting_results = []
        # Eg. -[nullslast]foo.bar.baz -> ("-", "nullslast", "foo.bar.", "baz")
        pattern = re.compile(
            "(-?)"
            r"(?:\[((?:nullsfirst)|(?:nullslast))\])?"
            r"((?:\w+\.)*)"
            r"(\w+)"
        )
        for sort_field in self.qs['sort'].split(','):
            # Parsing
            match = pattern.match(sort_field)
            if match is None:
                raise InvalidSort("Invalid sort field format {sort_field}")
            minus, nulls, join_path, schema_field = match.groups()
            order = 'desc' if minus else 'asc'
            schema_joins = join_path.strip(".").split(".") if join_path else []

            # Convert schema -> model relationships/fields
            cur_sch = self.schema
            model_joins = []
            for join in schema_joins:
                if join not in get_relationships(cur_sch):
                    raise InvalidSort("{} has no relationship {}".format(cur_sch.__name__, join))
                relationship = cur_sch._declared_fields[join]
                if relationship.many:
                    raise InvalidSort("Cannot do X->many join from {} to {}".format(cur_sch.__name__, join))
                model_joins.append(get_model_field(cur_sch, join))
                cur_sch = type(get_related_schema(cur_sch, join))

            if schema_field not in cur_sch._declared_fields:
                raise InvalidSort("{} has no attribute {}".format(cur_sch.__name__, schema_field))
            elif schema_field in get_relationships(cur_sch):
                raise InvalidSort("You can't sort on {} because it is a relationship field".format(schema_field))
            model_field = get_model_field(cur_sch, schema_field)

            sorting_results.append({"field": model_field, "order": order, "nulls": nulls, "joins": model_joins})

        return sorting_results

    @property
    def include(self):
        """Return fields to include

        :return list: a list of include information
        """
        include_param = self.qs.get('include', [])

        if current_app.config.get('MAX_INCLUDE_DEPTH') is not None:
            for include_path in include_param:
                if len(include_path.split('.')) > current_app.config['MAX_INCLUDE_DEPTH']:
                    raise InvalidInclude("You can't use include through more than {} relationships"
                                         .format(current_app.config['MAX_INCLUDE_DEPTH']))

        return include_param.split(',') if include_param else []
