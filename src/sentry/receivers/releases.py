from __future__ import absolute_import, print_function

import re

from django.db import IntegrityError, transaction
from django.db.models.signals import post_save

from sentry.app import locks
from sentry.models import (
    Commit, Group, GroupCommitResolution, Release, TagValue
)
from sentry.tasks.clear_expired_resolutions import clear_expired_resolutions
from sentry.utils.retries import TimedRetryPolicy


def ensure_release_exists(instance, created, **kwargs):
    if instance.key != 'sentry:release':
        return

    if instance.data and instance.data.get('release_id'):
        return

    affected = Release.objects.filter(
        organization_id=instance.project.organization_id,
        version=instance.value,
        projects=instance.project
    ).update(date_added=instance.first_seen)
    if not affected:
        release = Release.objects.filter(
            organization_id=instance.project.organization_id,
            version=instance.value
        ).first()
        if release:
            release.update(date_added=instance.first_seen)
        else:
            lock_key = Release.get_lock_key(instance.project.organization_id, instance.value)
            lock = locks.get(lock_key, duration=5)
            with TimedRetryPolicy(10)(lock.acquire):
                try:
                    release = Release.objects.get(
                        organization_id=instance.project.organization_id,
                        version=instance.value,
                    )
                except Release.DoesNotExist:
                    release = Release.objects.create(
                        organization_id=instance.project.organization_id,
                        version=instance.value,
                        date_added=instance.first_seen,
                    )
                    instance.update(data={'release_id': release.id})
        release.add_project(instance.project)


def resolve_group_resolutions(instance, created, **kwargs):
    if not created:
        return

    clear_expired_resolutions.delay(release_id=instance.id)


def resolved_in_commit(instance, created, **kwargs):
    # TODO(dcramer): we probably should support an updated message
    if not created:
        return

    match = re.search(r'\bFixes ([A-Za-z0-9_-]+-[A-Z0-9]+)\b')
    if not match:
        return

    short_id = match.group(1)
    try:
        group = Group.objects.get(
            project__organization=instance.organization_id,
            short_id=short_id,
        )
    except Group.DoesNotExist:
        return

    try:
        with transaction.atomic():
            GroupCommitResolution.objects.create(
                group_id=group.id,
                commit_id=instance.id,
            )
    except IntegrityError:
        pass


post_save.connect(
    resolve_group_resolutions,
    sender=Release,
    dispatch_uid="resolve_group_resolutions",
    weak=False
)


post_save.connect(
    ensure_release_exists,
    sender=TagValue,
    dispatch_uid="ensure_release_exists",
    weak=False
)


post_save.connect(
    resolved_in_commit,
    sender=Commit,
    dispatch_uid="resolved_in_commit",
    weak=False,
)
