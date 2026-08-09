import dlt

from dlt_framework.core.contracts import define_source
from dlt_framework.core.models import ResourcePolicy, SourceContext


@dlt.source(name="alpha")
def source(context: SourceContext):
    return dlt.resource(
        [{"id": 1, "value": "alpha"}],
        name="alpha_rows",
        write_disposition="append",
    )


SOURCE = define_source(
    factory=source,
    resources={"alpha_rows": ResourcePolicy(empty="fail")},
)
