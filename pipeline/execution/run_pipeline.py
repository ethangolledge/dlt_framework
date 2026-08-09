import argparse
import logging
from collections.abc import Sequence

from pipeline.core.configuration import load_pipeline_config
from pipeline.core.discovery import discover_source
from pipeline.core.errors import FrameworkError
from pipeline.core.runtime import run


LOGGER = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Load one source module into the configured destination."
    )
    parser.add_argument(
        "source",
        metavar="SOURCE",
        help="Source name to discover, for example accounts; qualify duplicates as rest/accounts",
    )
    args = parser.parse_args(argv)

    try:
        definition = discover_source(args.source)
        config = load_pipeline_config(definition)
        load_info = run(config)
    except FrameworkError as error:
        LOGGER.error("%s", error)
        return 2
    except Exception:
        LOGGER.exception("Pipeline run failed")
        return 1

    LOGGER.info("%s", load_info)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(main())
