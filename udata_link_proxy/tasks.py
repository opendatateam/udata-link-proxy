# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging

import requests

from udata.models import Dataset
from udata.tasks import job
from udata.utils import get_by

log = logging.getLogger(__name__)

LINK_PROXY_URL = 'http://datagouv.link-proxy.geo.data.gouv.fr'


@job('link-proxy-check')
def check(self, did, rid, url):
    log.debug('Sending check for url %s' % url)
    r = requests.post('%s/' % LINK_PROXY_URL, json={
        'location': url,
    })
    if not r.status_code == 200:
        log.error('link-proxy responded w/ status code %s' % r.status_code)
        return
    data = r.json()
    if '_id' not in data:
        log.error('link-proxy did not respond with an _id (%s)' % data)
        return
    check_id = data['_id']
    dataset = Dataset.objects.get(id=did)
    resource = get_by(dataset.resources, 'id', rid)
    resource.extras['link_proxy:check_id'] = check_id
    resource.save(signal_kwargs={'ignores': ['post_save']})
    log.debug('Check sent for url %s with id %s' % (url, check_id))
