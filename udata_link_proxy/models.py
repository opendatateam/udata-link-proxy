# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import re
import logging

from udata.models import Dataset
from udata.utils import get_by

from .tasks import check

log = logging.getLogger(__name__)


@Dataset.on_resource_added.connect
def on_resource_created(dataset, resource_id=None):
    log.debug('on_resource_created triggered in link_proxy')
    if not resource_id:
        log.error('No resource_id provided')
        return
    resource = get_by(dataset.resources, 'id', resource_id)
    log.debug('link_proxy sending check for %s' % resource.url)
    check.delay(dataset.id, resource.id, resource.url)


@Dataset.on_update.connect
def on_dataset_updated(dataset):
    updates, removals = dataset._delta()
    r = re.compile('resources\.([0-9])*\.url$')
    match = [re.match(r, k) for k in updates.keys()]
    match = [m.group(1) for m in match if m]
    for m in match:
        resource = dataset.resources[int(m)]
        log.debug('link_proxy sending check for %s from on_dataset_updated'
                  % resource.url)
        check.delay(dataset.id, resource.id, resource.url)
