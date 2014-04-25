:mod:`streamcorpus` --- single-document data representation
===========================================================

The core data representation for individual documents is a
:class:`~streamcorpus.StreamItem`.  This includes both storage for the
entire body contents in several forms and storage for tokenizers that
parse the document.  This is a pure in-memory form, but generated with
Apache Thrift, so that it can be serialized to network storage or
on-disk :class:`~streamcorpus.Chunk` files.

.. Since the core of this is in Thrift files which are arguably
   platform-independent, it's hard to get good inline documentation.
   So everything goes in this file.

In addition to the current version, there are two previous versions of
the :class:`~streamcorpus.StreamItem` structure, and files in the
previous format are stored in places such as Amazon S3 public data
sets.

Stream items
------------

There are many parts to this, loosely organized as follows:

* A :class:`~streamcorpus.StreamItem` contains metadata, plus the
  :attr:`~streamcorpus.StreamItem.body`
  :class:`~streamcorpus.ContentItem`.  There are
  :class:`~streamcorpus.Rating` objects giving the relevance of the
  entire document to chosen target entities.

* A :class:`~streamcorpus.ContentItem` contains the actual data,
  generally progressing from :attr:`~streamcorpus.ContentItem.raw`
  form to :attr:`~streamcorpus.ContentItem.clean_html` and then
  :attr:`~streamcorpus.ContentItem.clean_visible` forms.  An NLP
  tagger produces :attr:`~streamcorpus.ContentItem.sentences` from
  these.  Annotators can add :attr:`~streamcorpus.ContentItem.labels`
  to content items.

* A :class:`~streamcorpus.Sentence` just contains an ordered list of
  :attr:`~streamcorpus.Sentence.tokens`, and may again be annotated
  with :attr:`~streamcorpus.Sentence.labels`.

.. class:: streamcorpus.StreamItem(version=1, doc_id=None, abs_url=None, schost=None, original_url=None, source=None, body=None, source_metadata={}, stream_id=None, stream_time=None, other_content={}, rating={}, external_ids={})

    This is the primary interface to the corpus data.  It is a
    snapshot of a single document, identified by its URL, at a single
    point in time.  All corpora are stream corpora, even if they were
    not explicitly created as such.

    .. attribute:: version

        Version of this stream item, always
        :attr:`streamcorpus.Versions.v0_3_0`.

    .. attribute:: doc_id

        Identifier of the document, which may change over time, and of
        which this stream item is a snapshot.  This is always an MD5
        hash of :attr:`abs_url`.

    .. attribute:: abs_url

        Normalized form of the original document URL.

    .. attribute:: schost

        ``scheme://hostname`` part of :attr:`abs_url`.

    .. attribute:: original_url

        Original URL of the document.  This is only present of the
        original URL is not a valid canonicalized URL, in which case
        :attr:`abs_url` will be derived from this field.

    .. attribute:: source

        String uniquely identifying this data set.  Should start with
        a year string, such as "news" or "social".

    .. attribute:: body

        :class:`streamcorpus.ContentItem` holding the primary content
        of the stream item.

    .. attribute:: source_metadata

        Dictionary mapping strings to arbitrary binary data.  The keys
        should be short, descriptive, and free of whitespace.  In many
        cases the keys will be the same as the :attr:`source` of this
        stream item, and the values will be serialized JSON matching a
        metadata schema from <http://trec-kba.org/schemas/v1.0/>.
        ``http_headers`` is also an expected key.

    .. attribute:: stream_id

        Unique identifier for a stream item, snapshotting a document
        at a point in time.  This should be constructed as::

            si.stream_id = '%d-%s'.format(si.stream_time.epoch_ticks, si.doc_id)

    .. attribute:: stream_time

        :class:`streamcorpus.StreamTime` identifying the earliest time
        that this content was known to exist.  Usually, :attr:`body`
        was saved at the time of that first observation.

    .. attribute:: other_content

        Dictionary mapping strings to
        :class:`streamcorpus.ContentItem` for additional data attached
        to this stream item.  Typical keys are ``title``, ``anchor``,
        or ``extracted``.  ``anchor`` should map to the single anchor
        text of a hyperlink pointing to this documentation.

    .. attribute:: ratings

        Dictionary mapping annotator ID strings to lists of
        :class:`streamcorpus.Rating` judging the relationship of this
        entire document to some target entity.

    .. attribute:: external_ids

        Two-level map from system identifier to document or stream ID
        to some external identifier.

.. class:: streamcorpus.Rating

    Ratings are human-generated assertions about an entire document's
    utility for a particular topic or entity in a reference knowledge
    base.

    .. attribute:: annotator

        The :class:`~streamcorpus.Annotator` that produced this rating.

    .. attribute:: target

        The :class:`~streamcorpus.Target` of the rating.

    .. attribute:: relevance

        Numerical score assigned by annotator to "judge" or "rate" the
        utility of this stream item to addressing the target
        information need.  The range and interpretation of relevance
        numbers depends on the annotator.  This value can represent a
        rank ordering or an enumeration such as -1=Garbage, 0=Neutral,
        1=Useful, 2=Vital.

    .. attribute:: contains_mention

        Boolean indication of whether the document actually mentions
        the target entity.  This is only partially correlated with
        relevance.  For example, a document might mention the entity
        but only in "chrome" text, making it a Garbage-rated text for
        that entity.

    .. attribute:: comments

        Any additional notes provided by the annotator.

    .. attribute:: mentions

        Specific strings that correspond to the entity in text.

    .. attribute:: flags

        List of enumeration flags from
        :class:`~streamcorpus.FlagType`; for instance,
        :attr:`streamcorpus.FlagType.PROFILE` to indicate that this
        stream item is a profile for the target entity.

.. class:: streamcorpus.ContentItem

    A complete representation of the item's data.  For instance, the
    :attr:`streamcorpus.StreamItem.body` is a content item.

    .. attribute:: raw

        The original download as an unprocessed byte array.

    .. attribute:: encoding

        Character encoding, either guessed or determined from protocol
        headers.

    .. attribute:: media_type

        MIME type of the document, possibly from an HTTP or similar
        ``Content-Type:`` header

    .. attribute:: clean_html

        HTML-formatted version of :attr:`raw`.  This has correct UTF-8
        encoding and no broken tags.  All HTML-escaped characters are
        converted to their UTF-8 equivalents, and ``<``, ``>``, and
        ``&`` are escaped.

    .. attribute:: clean_visible

        Copy of :attr:`clean_html` with all HTML tags replaced with
        whitespace.  Byte offsets in this and :attr:`clean_html` are
        identical.  ``<``, ``>``, and ``&`` remain HTML-escaped.  This
        text can be directly inserted into an HTML or XML document
        without further escaping.

    .. attribute:: logs

        List of string log messages produced from the processing
        pipeline

    .. attribute:: taggings

        A dictionary mapping string tagger IDs to
        :class:`~streamcorpus.Tagging` objects.  This is a set of
        auto-generated taggings, such as a one-word-per-line (OWLP)
        tokenization and sentence chunking with part-of-speech,
        lemmatization, and NER classification.  The dictionary key
        should match the :attr:`streamcorpus.Tagging.tagger_id`, and
        also match the key in :attr:`sentences` and
        :attr:`sentence_blobs`, which come from transforming a
        :attr:`streamcorpus.Tagging.raw_tagging` into
        :class:`~streamcorpus.Sentence` and
        :class:`~streamcorpus.Token` instances.

        Taggings are generated from :attr:`clean_visible`, and offsets
        of all types refer to the :attr:`clean_visible` or
        :attr:`clean_html` version of the data, not :attr:`raw`.

    .. attribute:: labels

        A dictionary mapping string annotator IDs to lists of
        :class:`~streamcorpus.Label` annotations on the entire content item.

    .. attribute:: sentences

        A dictionary mapping tagger IDs to ordered lists of
        :class:`~streamcorpus.Sentence` objects produced by that
        tagger.

    .. attribute:: sentence_blobs

        A dictionary the same as :attr:`sentences`, but the dictionary
        values are Thrift-serialized binary strings that can be
        deserialized into the lists on demand.

    .. attribute:: language

        The :class:`~streamcorpus.Language` of the text.

    .. attribute:: relations

        A dictionary mapping tagger IDs to lists of
        :class:`~streamcorpus.Relation` identified by the tagger.

    .. attribute:: attributes

        A dictionary mapping tagger IDs to lists of
	:class:`~streamcorpus.Attribute` identified by the tagger.

    .. attribute:: external_ids

        A dictionary mapping tagger IDs to dictionaries mapping
        numeric mention IDs to text.  This allows external systems to
        associate record IDs with individual mentions or sets of
        mentions.

.. class:: streamcorpus.Annotator

    Description of a human (or a set of humans) that generated the
    data in a :class:`~streamcorpus.Label` or
    :class:`~streamcorpus.Rating`.

    .. attribute:: annotator_id

        Name of the annotator.  This is also used as the source key in
        several maps, and it is important for annotator IDs to be both
        consistent across annotations and unique.  We use the
        following conventions:

        * Avoid whitespace.

        * An email address is the best identifier.

        * Where a single email address is not appropriate, create a
          descriptive string, e.g. ``nist-trec-kba-2012-assessors``.

        * ``author`` means the person who wrote the original text.

    .. attribute:: annotation_time

        Approximate :class:`~streamcorpus.StreamTime` when the
        annotation was provided by a human.  This may be :const:`None`
        if no time is available.

.. class:: streamcorpus.Attribute

    Description of an attribute of an entity discovered by a tagger in the text.

    .. attribute:: attribute_type

        The :class:`~streamcorpus.AttributeType` of the attribute.

    .. attribute:: evidence

        String presented by the tagger as evidence of the attribute.

    .. attribute:: value

        Normalized, typed string value derived from :attr:`evidence`.
        The type of this is determined by :attr:`attribute_type`.  If
        the type is :attr:`streamcorpus.AttributeType.PER_GENDER`, for
        instance, this value will be a string containing an integer
        value from the :class:`streamcorpus.Gender` enumeration.  If
        the type implies a date-time type, the value is a
        :attr:`streamcorpus.StreamTime.zulu_timestamp`.

    .. attribute:: sentence_id

        Zero-based index into the sentences array for this tagger.

    .. attribute:: mention_id

        Index into the mentions in this document.  This identifies the
        mention to which the attribute applies.

.. class:: streamcorpus.Label

    Labels are human-generated assertions about a portion of a
    document.  For example, a human author might label their own text
    by inserting hyperlinks to Wikipedia, or a NIST assessor might
    record which tokens in a text mention a target entity.

    Labels appear in lists as the value parts of
    :attr:`streamcorpus.Token.labels`,
    :attr:`streamcorpus.Sentence.labels`, and
    :attr:`streamcorpus.ContentItem.labels`.

    .. attribute:: annotator

        The :class:`~streamcorpus.Annotator` source of this label.

    .. attribute:: target

        The :class:`~streamcorpus.Target` entity of this label.

    .. attribute:: offsets

        Map of :class:`~streamcorpus.OffsetType` enum value to
        :class:`~streamcorpus.Offset` describing what is labeled.

    .. attribute:: positive

        Labels are usually positive assertions that the token mentions
        the target.  It is sometimes useful to collect negative
        assertions that a token is not the target, which can be
        indicated by setting this field to :const:`False`.

    .. attribute:: comments

        Additional notes from the annotator about this label.

    .. attribute:: mentions

        List of strings that are mentions of this target in the text.

    .. attribute:: relevance

        Numerical score assigned by annotator to "judge" or "rate" the
        utility of this stream item to addressing the target
        information need.  The range and interpretation of relevance
        numbers depends on the annotator.  This value can represent a
        rank ordering or an enumeration such as -1=Garbage, 0=Neutral,
        1=Useful, 2=Vital.

    .. attribute:: stream_id

        :attr:`streamcorpus.StreamItem.stream_id` of the stream item
        containing this label.

    .. attribute:: flags

        List of integer :class:`~streamcorpus.FlagType` enumeration
        values further describing this label.

.. class:: streamcorpus.Target

    An entity or topic being identified by a
    :class:`~streamcorpus.Label`.  These often come from a knowledge
    base such as Wikipedia.

    .. attribute:: target_id

        Unique string identifier for the target.  This can be a URL
        from a Wikipedia, Freebase, or other structured reference
        system about the target.

    .. attribute:: kb_id

        Optional string identifying the knowledge base, if it is not
        obvious from :attr:`target_id`.

    .. attribute:: kb_snapshot_time

        :class:`~streamcorpus.StreamTime` when the knowledge base
        article was accessed.

.. class:: streamcorpus.FlagType

    General-purpose flags.  These are integer values.

    .. attribute:: PROFILE

        This label is to a profile for the target in a knowledge base.

.. class:: streamcorpus.StreamTime

    Time attached to a stream item.

    .. attribute:: epoch_ticks

    Time, in fractional seconds, since midnight 1 Jan 1970 UTC ("Unix
    time"), the same time as returned by :func:`time.time`.

    .. attribute:: zulu_timestamp

    Formatted version of the time string.

.. class:: streamcorpus.Offset

    A range of data within some :class:`~streamcorpus.ContentItem`.

    .. attribute:: type

        The :class:`~streamcorpus.OffsetType` value that describes
        what units this offset uses; for instance,
        :attr:`streamcorpus.OffsetType.CHARS`.

    .. attribute:: first

        First item in the range.

    .. attribute:: length

        Length of the range.

            data = content_item[offset.first:offset.first+offset.length]

    .. attribute:: xpath

        An optional string giving an XPath query into an XML or XHTML
        document.

    .. attribute:: content_form

        If a :class:`~streamcorpus.ContentItem` has multiple forms,
        the name of the form, such as "raw", "clean_html", or
        "clean_visible".  "clean_visible" is the most common case.

    .. attribute:: value

        The actual content of the range.  Only present as a debugging
        aid and frequently empty.

.. class:: streamcorpus.OffsetType

    Part of an :class:`~streamcorpus.Offset` that describes what units
    the offset uses.

    .. attribute:: LINES

        The offset is a range of line numbers.

    .. attribute:: BYTES

        The offset is a range of bytes.

    .. attribute:: CHARS

        The offset is a range of characters, typically in Unicode.

.. class:: streamcorpus.Tagging

    Information about and output from a tagging tool.

    .. attribute:: tagger_id

        Opaque string identifying the tagger.

    .. attribute:: raw_tagging

        Raw output of the tagging tool.

    .. attribute:: tagger_config

        Short, human-readable description of the configuration parameters.

    .. attribute:: tagger_version

        Short, human-readable version string of the tagging tool.

    .. attribute:: generation_time

        :class:`~streamcorpus.StreamTime` the tagging was generated.

.. class:: streamcorpus.Relation

    Description of a relation between two entities that a tagger
    discovered in the text.

    If a tagger discovers that Bob is located in Chicago, then
    :attr:`relation_type` would be
    :attr:`streamcorpus.RelationType.PHYS_Located`,
    :attr:`sentence_id_1` and :attr:`mention_id_1` would refer to
    "Bob", and :attr:`sentence_id_2` and :attr:`mention_id_2` would
    refer to "Chicago".

    .. attribute:: relation_type

        Type of the relation, one of the
        :class:`~streamcorpus.RelationType` enumeration values.

    .. attribute:: sentence_id_1

        Zero-based sentence index for the sentences for this tagger ID
        for the first entity.

    .. attribute:: mention_id_1

        Index into the mentions in the document for the first entity.

    .. attribute:: sentence_id_2

        Zero-based sentence index for the sentences for this tagger ID
        for the second entity.

    .. attribute:: mention_id_2

        Index into the mentions in the document for the second entity.

.. class:: streamcorpus.Language

    Description of a natural language used in the text.

    .. attribute:: code

        Two-letter code for the language, such as "en"

    .. attribute:: name

        Full name of the language

.. class:: streamcorpus.Sentence

    A complete sentence.

    .. attribute:: tokens

        List of :class:`~streamcorpus.Token` in the sentence.

    .. attribute:: labels

        Map of string annotator ID to list of
        :class:`~streamcorpus.Label` for labels over the entire
        sentence.

.. class:: streamcorpus.Token

    Textual tokens identified by an NLP pipeline and marked up with
    metadata from automatic taggers, and possibly also labels from
    humans.

    .. attribute:: token_num

        Zero-based index into the stream of tokens from a document.

    .. attribute:: token

        Actual token string; a UTF-8 encoded Python :func:`string`

    .. attribute:: offsets

        Location of the token in the original document.  This is a map
        from :class:`~streamcorpus.OffsetType` enum value to
        :class:`~streamcorpus.Offset`, allowing the token to have line
        number, byte position, and character position offsets.

    .. attribute:: sentence_pos

        Zero-based index into the sentence, or -1 if unavailable.

    .. attribute:: lemma

        Lemmatization of the token.

    .. attribute:: pos

        Part-of-speech label.  For possible values, see
	http://www.ling.upenn.edu/courses/Fall_2003/ling001/penn_treebank_pos.html

    .. attribute:: entity_type

        One of the :class:`~streamcorpus.EntityType` enumeration values.

    .. attribute:: mention_id

        Identifier for each mention in this tagger's description of
        the document.  This is unique for a single tagger within a
        single document.  -1 means "no mention".

        The mention ID distinguishes multi-token mentions; tokens that
        correspond to the same mention will have the same mention ID.
        This is needed when other fields do not change between tokens
        that are part of separate mentions: "the senator is known to
        his friends as David, Davy, Zeus, and Mr. Elephant".

	The mention ID identifies mentions in
	:class:`~streamcorpus.Relation` objects.

    .. attribute:: equiv_id

        Single-document coref chain ID.  This is unique for a single
        tagger within a single document.  Tokens that refer to the
        same entity will have the same equivalence ID.  -1 means
        "none".

    .. attribute:: parent_id

        Parent sentence position in the dependency parse.  -1 means
        "none".

    .. attribute:: dependency_path

        Grammatical relation label on the path to the parent in a
        dependency parse, defined by whatever tagger was used.

    .. attribute:: labels

        Dictionary mapping annotator ID strings to lists of
        :class:`~streamcorpus.Label` objects for labels on this token.

    .. attribute:: mention_type

        One of the :class:`~streamcorpus.MentionType` enumeration
        values.

    .. attribute:: custom_entity_type

        If :attr:`entity_type` is
        :attr:`streamcorpus.EntityType.CUSTOM_TYPE`, a string
        describing what exactly the entity type is.  This is useful
        when a specialized tagger has a large number of unique entity
        types, such as ``entity:artefact:weapon:blunt``.

.. autoclass:: streamcorpus.EntityType
    :members:
    :undoc-members:

.. autoclass:: streamcorpus.MentionType
    :members:
    :undoc-members:

.. autoclass:: streamcorpus.Gender
    :members:
    :undoc-members:

.. autoclass:: streamcorpus.AttributeType
    :members:
    :undoc-members:

.. autoclass:: streamcorpus.RelationType
    :members:
    :undoc-members:

Storage
-------

There are a number of helpers to assist with binary serialization and deserialization.

.. autoclass:: streamcorpus.Chunk
    :members:
    :undoc-members:

.. autofunction:: streamcorpus.decrypt_and_uncompress

.. autofunction:: streamcorpus.compress_and_encrypt

.. autofunction:: streamcorpus.compress_and_encrypt_path

.. autofunction:: streamcorpus.serialize

.. autofunction:: streamcorpus.deserialize

.. autoclass:: streamcorpus.VersionMismatchError
    :show-inheritance:

.. autofunction:: streamcorpus.get_date_hour

.. autofunction:: streamcorpus.make_stream_time

.. autofunction:: streamcorpus.make_stream_item

.. autofunction:: streamcorpus.add_annotation

.. autofunction:: streamcorpus.get_entity_type

:program:`streamcorpus_dump` tool
---------------------------------

:program:`streamcorpus_dump` prints information on a
:class:`streamcorpus.Chunk` file.  Basic usage is:

.. code-block:: bash

    streamcorpus_dump --show-all input.sc
