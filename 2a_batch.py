#!/usr/bin/env python
from __future__ import annotations

import os
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from .shared import NEWLINE, calc_you_later

if TYPE_CHECKING:
    from typing import Iterable

    from .shared import CitiesType


def update_cities(city: str, temp: str | float | list[float], cities: CitiesType) -> CitiesType:
    if isinstance(temp, (str, int)):
        temp = [float(temp)]
    elif isinstance(temp, float):
        temp = [temp]

    if city not in cities:
        cities[city] = temp
    else:
        cities[city].extend(temp)

    return cities


def process_batch(batch: bytes) -> CitiesType:
    cities: CitiesType = {}
    for line in batch.decode("utf-8").splitlines():
        if ";" not in line:
            continue
        city, temp = line.split(";")
        cities = update_cities(city, temp, cities)
    return cities


def merge_batch(batch: CitiesType, cities: CitiesType) -> CitiesType:
    for city, temps in batch.items():
        existing = cities.get(city)
        if existing:
            cities[city] = [*existing, *temps]
        else:
            cities[city] = temps

    return cities


def batched(path: str | Path) -> Iterable[bytes]:
    # file_size: int = os.stat(path).st_size
    chunksize: int = 1_073_741_824 // 2
    chunk: bytes = b""

    with open(path, "rb", buffering=0) as handle:
        while batch := handle.read(chunksize):
            if batch[-1] == NEWLINE:
                yield batch
            else:
                chunk = b""
                while char := handle.read(1):
                    chunk += char
                    if char == NEWLINE:
                        break
                yield batch + chunk


def main():
    path = Path("measurements.txt")
    # path = Path("sample.txt")
    print(f"{datetime.now()} - processing start")
    cities: CitiesType = {}
    threads: int = os.process_cpu_count() or 2
    cores: int = threads // 2
    with ProcessPoolExecutor(max_workers=threads) as executor:
        for batch in executor.map(process_batch, batched(path)):
            cities = merge_batch(batch, cities)
    print(f"{datetime.now()} - processing end")
    print(f"{datetime.now()} - calculating start")
    _ = calc_you_later(cities)
    print(f"{datetime.now()} - calculating end")


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    print(end - start)
