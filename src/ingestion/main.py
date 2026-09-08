"""Einstiegspunkt fuer beide Producer-Modi.

Ein Image, zwei Modi, zwei Deployments. Die Modi teilen sich Serialisierung,
Registry-Client und Kafka-Konfiguration (common.py) und unterscheiden sich
nur in der Datenquelle — deshalb ein gemeinsames Artefakt statt zweier
Codebasen, in denen dieselbe Avro-Logik zweimal gepflegt werden muesste.

Getrennt deployt werden sie trotzdem, weil ihre Skalierungssemantik
gegenlaeuefig ist: der Lastgenerator soll auf viele Repliken hochgehen, der
Live-Poller darf das nur geshardet.
"""

from __future__ import annotations

import logging
import sys

from common import Settings, setup_logging

log = logging.getLogger("ingestion.main")


def main() -> int:
    setup_logging()
    settings = Settings.from_env()

    if settings.mode == "synthetic":
        import synthetic

        synthetic.run(settings)
    elif settings.mode == "live":
        import live_poller

        live_poller.run(settings)
    else:
        log.error("Unbekannter MODE=%r, erlaubt: synthetic | live", settings.mode)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
