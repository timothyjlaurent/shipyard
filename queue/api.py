# Copyright 2013 Evan Hazlett and contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from tastypie import fields
from tastypie.resources import ModelResource, Resource
from tastypie.authorization import Authorization
from tastypie.authentication import (ApiKeyAuthentication,
    SessionAuthentication, MultiAuthentication)
from tastypie.bundle import Bundle
from django.conf.urls import url
from django.http import Http404, HttpResponse
from django.conf import settings
from tastypie.utils import trailing_slash
from shipyard import utils
from queue.models import QUEUE_KEY
import json
import redis

class TaskObject(object):
    """
    Wrapper around task in Redis
    """
    def __init__(self, initial=None):
        self.__dict__['_data'] = {}

        if hasattr(initial, 'items'):
            self.__dict__['_data'] = initial

    def __getattr__(self, name):
        return self._data.get(name, None)

    def __setattr__(self, name, value):
        self.__dict__['_data'][name] = value

    def to_dict(self):
        return self._data

class TaskResource(Resource):
    id = fields.CharField(attribute='id')
    date = fields.CharField(attribute='date')
    host_id = fields.IntegerField(attribute='host_id')
    command = fields.CharField(attribute='command')
    ack = fields.BooleanField(attribute='ack')
    params = fields.DictField(attribute='params')

    class Meta:
        resource_name = 'tasks'
        object_class = TaskObject
        authorization = Authorization()
        authentication = MultiAuthentication(
            ApiKeyAuthentication(), SessionAuthentication())
        list_allowed_methods = ['get']
        detail_allowed_methods = ['get', 'put', 'post']
        filtering = {
            'host_id': ['exact'],
        }

    def _get_task(self, task_id=None):
        """
        Gets a task

        """
        key = QUEUE_KEY.format(task_id)
        rds = utils.get_redis_connection()
        data = rds.hgetall(key)
        if data.get('ack') == '0':
            data['ack'] = False
        else:
            data['ack'] = True
        if data.has_key('params'):
            data['params'] = json.loads(data['params'])
        # create TaskObject
        obj = TaskObject(initial=data)
        return obj

    def _get_tasks(self, show_all=False):
        """
        Gets pending tasks

        """
        key = QUEUE_KEY.format('*')
        rds = utils.get_redis_connection()
        all_keys = rds.keys(key)
        all_tasks = []
        for k in all_keys:
            t_id = k.split(':')[-1]
            task = self._get_task(t_id)
            if show_all or not task.ack:
                all_tasks.append(task)
        return all_tasks

    def detail_uri_kwargs(self, bundle_or_obj):
        kwargs = {}
        if isinstance(bundle_or_obj, Bundle):
            kwargs['pk'] = bundle_or_obj.obj.id
        else:
            kwargs['pk'] = bundle_or_obj.id
        return kwargs

    def get_object_list(self, request, bundle, **kwargs):
        show_all = bundle.request.GET.has_key('all')
        tasks = self._get_tasks(show_all=show_all)
        return tasks

    def obj_get_list(self, request=None, **kwargs):
        return self.get_object_list(request, **kwargs)

    def obj_get(self, request=None, **kwargs):
        id = kwargs.get('pk')
        obj = self._get_task(id)
        return obj

    def obj_create(self, bundle, request=None, **kwargs):
        raise NotImplementedError("object creation is not implemented")

    def obj_update(self, bundle, **kwargs):
        task_id = kwargs.get('pk')
        rds = utils.get_redis_connection()
        data = json.loads(bundle.request.body)
        task_data = self._get_task(task_id).to_dict()
        key = QUEUE_KEY.format(task_id)
        for k,v in data.iteritems():
            task_data[k] = v
        # convert to json for redis
        task_data['params'] = json.dumps(task_data['params'])
        task_data['ack'] = '1'
        rds.hmset(key, task_data)
        return bundle

    def obj_delete(self, request=None, **kwargs):
        raise NotImplementedError("object deletion is not implemented")

    def obj_delete_list(self, request=None, **kwargs):
        raise NotImplementedError("object deletion is not implemented")
