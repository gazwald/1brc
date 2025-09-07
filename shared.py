from statistics import mean

NEWLINE: bytes = b"\n"

CitiesType = dict[str, list[float]]
CitiesCalcType = dict[str, tuple[float, float, float]]


def calc_you_later(cities: CitiesType) -> CitiesCalcType:
    return {
        city: (
            min(temps),
            mean(temps),
            max(temps),
        )
        for city, temps in cities.items()
    }
