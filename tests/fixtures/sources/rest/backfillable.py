import dlt

from dlt_framework.core.contracts import define_source
from dlt_framework.core.models import ResourcePolicy, SourceContext

execution_windows = []


@dlt.source
def source(context: SourceContext):
    bounds = context.bounds_for("events")

    @dlt.resource(
        name="events",
        write_disposition="merge",
        primary_key="id",
    )
    def events():
        if bounds is None:
            yield {"id": "current", "window_start": None, "window_end": None}
            return
        execution_windows.append((bounds.start, bounds.end))
        yield {
            "id": bounds.start.isoformat(),
            "window_start": bounds.start,
            "window_end": bounds.end,
        }

    @dlt.resource(name="snapshot", write_disposition="replace")
    def snapshot():
        yield {"id": 1}

    resources = {"events": events, "snapshot": snapshot}
    if context.selected_resource is not None:
        return resources[context.selected_resource]
    return list(resources.values())


SOURCE = define_source(
    factory=source,
    resources={
        "events": ResourcePolicy(empty="fail", backfill=True),
        "snapshot": ResourcePolicy(empty="fail"),
    },
)
