from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsAllColumnsMissing,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
    BasePersistDocsQuotedColumnCaseSensitive,
    BasePersistDocsQuotedDescriptionNotAppliedOnMismatch,
)


class TestPersistDocs(BasePersistDocs):
    pass


class TestPersistDocsColumnMissing(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsAllColumnsMissing(BasePersistDocsAllColumnsMissing):
    pass


class TestPersistDocsQuotedColumnCaseSensitive(BasePersistDocsQuotedColumnCaseSensitive):
    pass


class TestPersistDocsQuotedDescriptionNotAppliedOnMismatch(
    BasePersistDocsQuotedDescriptionNotAppliedOnMismatch
):
    pass


class TestPersistDocsCommentOnQuotedColumn(BasePersistDocsCommentOnQuotedColumn):
    pass
