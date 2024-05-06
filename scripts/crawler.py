import json
from datetime import datetime
from pathlib import Path

import click
import requests_cache
import yfinance as yf  # type: ignore
from click.types import DateTime
from pyrate_limiter import Duration, Limiter, RequestRate
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from rich.progress import track

from src.services.cronjobs.stocks import TickersEnum


# Cache limiter session
class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass


session = CachedLimiterSession(
    limiter=Limiter(
        RequestRate(2, Duration.SECOND * 5)
    ),  # max 2 requests per 5 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)


def main(start: str, end: str, output_file: Path):
    for ticker in track(TickersEnum):
        ticker_name = ticker.name.lower()

        yf_ticker = yf.Ticker(ticker_name, session=session)
        data = yf_ticker.history(start=start, end=end).to_json()

        if data is not None:
            path = output_file.joinpath(ticker_name)
            if not path.exists():
                path.mkdir()

            json_data = json.loads(data)

            with open(path.joinpath(f"{start}_{end}.json"), "w") as f:
                json.dump(json_data, f)


@click.command()
@click.argument("start", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.argument("end", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.argument(
    "output_file",
    type=click.Path(
        exists=True, file_okay=False, dir_okay=True, writable=True, path_type=Path
    ),
)
def main_cli(start: datetime, end: datetime, output_file: Path):
    main(str(start.date()), str(end.date()), output_file)


if __name__ == "__main__":
    main_cli()
