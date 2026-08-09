import dlt

from dlt_framework.core.contracts import define_source
from dlt_framework.core.models import ResourcePolicy, SourceContext


@dlt.source
def source(context: SourceContext):
    return dlt.resource(
        [{"id": 1}],
        name="events",
        write_disposition="merge",
        primary_key="id",
    )


SOURCE = define_source(
    factory=source,
    resources={"events": ResourcePolicy(empty="allow", backfill=True)},
)
