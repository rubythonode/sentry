"""
sentry.plugins.base.structs
~~~~~~~~~~~~~~~~~~~~~~~~~~~

:copyright: (c) 2010-2013 by the Sentry Team, see AUTHORS for more details.
:license: BSD, see LICENSE for more details.
"""

from __future__ import absolute_import, print_function

__all__ = ['ReleaseHook']

from django.utils import timezone

from sentry.app import locks
from sentry.models import Activity, Release
from sentry.utils.retries import TimedRetryPolicy


class ReleaseHook(object):
    def __init__(self, project):
        self.project = project

    def start_release(self, version, **values):
        values.setdefault('date_started', timezone.now())

        affected = Release.objects.filter(
            version=version,
            organization_id=self.project.organization_id,
            projects=self.project,
        ).update(**values)
        if not affected:
            release = Release.objects.filter(
                version=version,
                organization_id=self.project.organization_id,
            ).first()
            if release:
                release.update(**values)
            else:
                lock_key = Release.get_lock_key(self.project.organization_id, version)
                lock = locks.get(lock_key, duration=5)
                with TimedRetryPolicy(10)(lock.acquire):
                    try:
                        release = Release.objects.get(
                            version=version,
                            organization_id=self.project.organization_id
                        )
                    except Release.DoesNotExist:
                        release = Release.objects.create(
                            version=version,
                            organization_id=self.project.organization_id,
                            **values
                        )

            release.add_project(self.project)

    # TODO(dcramer): this is being used by the release details endpoint, but
    # it'd be ideal if most if not all of this logic lived there, and this
    # hook simply called out to the endpoint
    def set_commits(self, version, commit_list):
        """
        Commits should be ordered oldest to newest.

        Calling this method will remove all existing commit history.
        """
        project = self.project
        release = Release.objects.filter(
            organization_id=project.organization_id,
            version=version,
            projects=self.project
        ).first()
        if not release:
            release = Release.objects.filter(
                organization_id=project.organization_id,
                version=version,
            ).first()
            if not release:
                lock_key = Release.get_lock_key(project.organization_id, version)
                lock = locks.get(lock_key, duration=5)
                with TimedRetryPolicy(10)(lock.acquire):
                    try:
                        release = Release.objects.get(
                            organization_id=project.organization_id,
                            version=version
                        )
                    except Release.DoesNotExist:
                        release = Release.objects.create(
                            organization_id=project.organization_id,
                            version=version
                        )
            release.add_project(project)

        release.set_commits(commit_list)

    def finish_release(self, version, **values):
        values.setdefault('date_released', timezone.now())
        affected = Release.objects.filter(
            version=version,
            organization_id=self.project.organization_id,
            projects=self.project,
        ).update(**values)
        if not affected:
            release = Release.objects.filter(
                version=version,
                organization_id=self.project.organization_id,
            ).first()
            if release:
                release.update(**values)
            else:
                lock_key = Release.get_lock_key(self.project.organization_id, version)
                lock = locks.get(lock_key, duration=5)
                with TimedRetryPolicy(10)(lock.acquire):
                    try:
                        release = Release.objects.get(
                            version=version,
                            organization_id=self.project.organization_id,
                        )
                    except Release.DoesNotExist:
                        release = Release.objects.create(
                            version=version,
                            organization_id=self.project.organization_id,
                            **values
                        )
            release.add_project(self.project)

        activity = Activity.objects.create(
            type=Activity.RELEASE,
            project=self.project,
            ident=version,
            data={'version': version},
            datetime=values['date_released'],
        )
        activity.send_notification()

    def handle(self, request):
        raise NotImplementedError
