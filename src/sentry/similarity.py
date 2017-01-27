from __future__ import absolute_import

import math
import random
import struct

import six


def scale_to_total(value):
    """\
    Convert a mapping of distinct quantities to a mapping of proportions of the
    total quantity.
    """
    total = float(sum(value.values()))
    return {k: (v / total) for k, v in value.items()}


def get_distance(target, other):
    """\
    Calculate the N-dimensional Euclidian between two mappings.

    The mappings are used to represent sparse arrays -- if a key is not present
    in both mappings, it's assumed to be 0 in the mapping where it is missing.
    """
    return math.sqrt(
        sum(
            (target.get(k, 0) - other.get(k, 0)) ** 2
            for k in set(target) | set(other)
        )
    )


formatters = sorted([
    (2 ** 8 - 1, struct.Struct('>B').pack),
    (2 ** 16 - 1, struct.Struct('>H').pack),
    (2 ** 32 - 1, struct.Struct('>L').pack),
    (2 ** 64 - 1, struct.Struct('>Q').pack),
])


def get_number_formatter(size):
    """\
    Returns a function that packs a number no larger than the provided size
    into to an efficient binary representation.
    """
    assert size > 0

    for maximum, formatter in formatters:
        if maximum >= size:
            return formatter

    raise ValueError('No registered formatter can handle the provided value.')


class MinHashIndex(object):
    """\
    Implements an index that can be used to efficiently search for items that
    share similar characteristics.

    This implementation is based on MinHash (which is used quickly identify
    similar items and estimate the Jaccard similarity of their characteristic
    sets) but this implementation extends the typical design to add the ability
    to record items by an arbitrary key. This allows querying for similar
    groups that contain many different characteristic sets.

    The ``rows`` parameter is the size of the hash ring used to collapse the
    domain of all tokens to a fixed-size range. The total size of the LSH
    signature is ``bands * buckets``. These attributes control the distribution
    of data within the index, and modifying them after data has already been
    written will cause data loss and/or corruption.

    This is modeled as two data structures:

    - A bucket frequency sorted set, which maintains a count of what buckets
      have been recorded -- and how often -- in a ``(band, key)`` pair. This
      data can be used to identify what buckets a key is a member of, and also
      used to identify the degree of bucket similarity when comparing with data
      associated with another key.
    - A bucket membership set, which maintains a record of what keys have been
      record in a ``(band, bucket)`` pair. This data can be used to identify
      what other keys may be similar to the lookup key (but not the degree of
      similarity.)
    """
    BUCKET_MEMBERSHIP = '0'
    BUCKET_FREQUENCY = '1'

    def __init__(self, cluster, rows, bands, buckets):
        self.namespace = b'sim'

        self.cluster = cluster
        self.rows = rows

        generator = random.Random(0)

        def shuffle(value):
            generator.shuffle(value)
            return value

        self.bands = [
            [shuffle(range(rows)) for _ in xrange(buckets)]
            for _ in xrange(bands)
        ]

        self.__bucket_formatter = get_number_formatter(rows)

    def __format_buckets(self, bucket):
        return b''.join(
            map(self.__bucket_formatter, bucket)
        )

    def get_signature(self, value):
        """Generate a minhash signature for a value."""
        columns = set(hash(token) % self.rows for token in value)
        return map(
            lambda band: map(
                lambda permutation: next(i for i, a in enumerate(permutation) if a in columns),
                band,
            ),
            self.bands,
        )

    def get_similarity(self, target, other):
        """\
        Calculate the degree of similarity between two bucket frequency
        sequences which represent two different keys.

        This is mainly an implementation detail for sorting query results, but
        is exposed publically for testing. This method assumes all input
        values have already been normalized using ``scale_to_total``.
        """
        assert len(target) == len(other)
        assert len(target) == len(self.bands)
        return 1 - sum(
            map(
                lambda (left, right): get_distance(
                    left,
                    right,
                ),
                zip(target, other)
            )
        ) / math.sqrt(2) / len(target)

    def query(self, scope, key):
        """\
        Find other entries that are similar to the one repesented by ``key``.

        This returns an sequence of ``(key, estimated similarity)`` pairs,
        where a similarity score of 1 is completely similar, and a similarity
        score of 0 is completely dissimilar. The result sequence is ordered
        from most similar to least similar. (For example, the search key itself
        isn't filtered from the result and will always have a similarity of 1,
        typically making it the first result.)
        """
        def fetch_bucket_frequencies(keys):
            """Fetch the bucket frequencies for each band for each provided key."""
            with self.cluster.map() as client:
                responses = {
                    key: map(
                        lambda band: client.zrange(
                            b'{}:{}:{}:{}:{}'.format(self.namespace, scope, self.BUCKET_FREQUENCY, band, key),
                            0,
                            -1,
                            desc=True,
                            withscores=True,
                        ),
                        range(len(self.bands)),
                    ) for key in keys
                }

            result = {}
            for key, promises in responses.items():
                # Resolve each promise, and scale the number of observations
                # for each bucket to [0,1] value (the proportion of items
                # observed in that band that belong to the bucket for the key.)
                result[key] = map(
                    lambda promise: scale_to_total(
                        dict(promise.value)
                    ),
                    promises,
                )

            return result

        def fetch_candidates(signature):
            """Fetch all the similar candidates for a given signature."""
            with self.cluster.map() as client:
                responses = map(
                    lambda (band, buckets): map(
                        lambda bucket: client.smembers(
                            b'{}:{}:{}:{}:{}'.format(self.namespace, scope, self.BUCKET_MEMBERSHIP, band, bucket)
                        ),
                        buckets,
                    ),
                    enumerate(signature),
                )

            # Resolve all of the promises for each band and reduce them into a
            # single set per band.
            return map(
                lambda band: reduce(
                    lambda values, promise: values | promise.value,
                    band,
                    set(),
                ),
                responses,
            )

        target_frequencies = fetch_bucket_frequencies([key])[key]

        # Flatten the results of each band into a single set. (In the future we
        # might want to change this to only calculate the similarity for keys
        # that show up in some threshold number of bands.)
        candidates = reduce(
            lambda total, band: total | band,
            fetch_candidates(target_frequencies),
            set(),
        )

        return sorted(
            map(
                lambda (key, candidate_frequencies): (
                    key,
                    self.get_similarity(
                        target_frequencies,
                        candidate_frequencies,
                    ),
                ),
                fetch_bucket_frequencies(candidates).items(),
            ),
            key=lambda (key, similarity): (similarity * -1, key),
        )

    def record(self, scope, key, characteristics):
        """Records the presence of a set of characteristics within a group."""
        with self.cluster.map() as client:
            for band, buckets in enumerate(self.get_signature(characteristics)):
                buckets = self.__format_buckets(buckets)
                client.sadd(
                    b'{}:{}:{}:{}:{}'.format(self.namespace, scope, self.BUCKET_MEMBERSHIP, band, buckets),
                    key,
                )
                client.zincrby(
                    b'{}:{}:{}:{}:{}'.format(self.namespace, scope, self.BUCKET_FREQUENCY, band, key),
                    buckets,
                    1,
                )


import itertools

from sentry.models import Event
from sentry.utils import redis
from sentry.utils.iterators import shingle


class GroupFeature(object):
    def __init__(self, index, scope, key, characteristics):
        self.index = index
        self.scope = scope
        self.key = key
        self.characteristics = characteristics

    def __flatten_sequence(self, sequence):
        return ':'.join(sequence)

    def query(self, group):
        return self.index.query(
            self.__flatten_sequence(self.scope(group)),
            self.__flatten_sequence(self.key(group)),
        )

    def record(self, event):
        return self.index.record(
            self.__flatten_sequence(self.scope(event.group)),
            self.__flatten_sequence(self.key(event.group)),
            self.characteristics(event),
        )


class GroupFeatureManager(object):
    def __init__(self, features):
        self.features = features

    def get(self, label):
        return self.features[label]

    def query(self, group):
        results = {}
        for label, feature in self.features.items():
            for key, similarity in feature.query(group):
                results.setdefault(key, {})[label] = similarity
        return sorted(
            results.items(),
            key=lambda (key, similarities): sum(similarities.values()),
            reverse=True,
        )

    def record(self, event):
        Event.objects.bind_nodes([event], 'data')

        results = []
        for label, feature in self.features.items():
            # XXX: This is really bad and should use futures instead.
            try:
                results.append((True, feature.record(event)))
            except Exception as error:
                results.append((False, error))

        return results


def tokenize_frame(frame):
    return '{module}.{function}'.format(
        module=frame.get('module', '?'),
        function=frame.get('function', '?'),
    )


features = GroupFeatureManager({
    'application-frames': GroupFeature(
        MinHashIndex(redis.clusters.get('default'), 0xFFFF, 8, 2),
        scope=lambda group: ['af', six.text_type(group.project_id)],
        key=lambda group: [six.text_type(group.id)],
        characteristics=lambda event: itertools.imap(
            lambda (in_app, frames): tuple(
                map(
                    tokenize_frame,
                    frames,
                )
            ),
            itertools.ifilter(
                lambda (in_app, frames): in_app,
                itertools.groupby(
                    event.data['sentry.interfaces.Exception']['values'][0]['stacktrace']['frames'],
                    key=lambda frame: frame.get('in_app', False),
                )
            ),
        ),
    ),
    'frames': GroupFeature(
        MinHashIndex(redis.clusters.get('default'), 0xFFFF, 8, 2),
        scope=lambda group: ['f', six.text_type(group.project_id)],
        key=lambda group: [six.text_type(group.id)],
        characteristics=lambda event: shingle(
            3,
            map(
                tokenize_frame,
                event.data['sentry.interfaces.Exception']['values'][0]['stacktrace']['frames'],
            ),
        )
    ),
    'message': GroupFeature(
        MinHashIndex(redis.clusters.get('default'), 0xFFFF, 8, 2),
        scope=lambda group: ['m', six.text_type(group.project_id)],
        key=lambda group: [six.text_type(group.id)],
        characteristics=lambda event: shingle(
            3,
            event.data['sentry.interfaces.Exception']['values'][0]['value'].split(),
        )
    ),
})
