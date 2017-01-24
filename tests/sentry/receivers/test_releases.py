from __future__ import absolute_import

from mock import patch

from sentry.models import (
    Commit, GroupCommitResolution, Release, TagValue
)
from sentry.testutils import TestCase


class EnsureReleaseExistsTest(TestCase):
    def test_simple(self):
        tv = TagValue.objects.create(
            project=self.project,
            key='sentry:release',
            value='1.0',
        )

        tv = TagValue.objects.get(id=tv.id)
        assert tv.data['release_id']

        release = Release.objects.get(
            id=tv.data['release_id']
        )
        assert release.version == tv.value
        assert release.projects.first() == self.project
        assert release.organization == self.project.organization

        # ensure we dont hit some kind of error saving it again
        tv.save()


class ResolveGroupResolutionsTest(TestCase):
    @patch('sentry.tasks.clear_expired_resolutions.clear_expired_resolutions.delay')
    def test_simple(self, mock_delay):
        release = Release.objects.create(
            version='a',
            organization_id=self.project.organization_id,
        )
        release.add_project(self.project)

        mock_delay.assert_called_once_with(
            release_id=release.id,
        )


class ResolvedInCommitTest(TestCase):
    # TODO(dcramer): pull out short ID matching and expand regexp tests
    def test_simple(self):
        group = self.create_group()

        commit = Commit.objects.create(
            message='Foo Biz\n\nFixes {}'.format(group.short_id),
        )

        assert GroupCommitResolution.objects.filter(
            group_id=group.id,
            commit_id=commit.id,
        ).exists()
