# app/services/accumulator_service.py

class Accumulator:
    def __init__(self):
        self.calc_accum = 0.0
        self.real_accum = 0.0
        self.explained_accum = 0.0

        self.calc_series = []
        self.real_series = []
        self.explained_series = []

    def add(self, calc, real_total, explained_total):
        # Calculada
        if calc is not None:
            self.calc_accum += float(calc)

        # Real
        if real_total is not None:
            self.real_accum += float(real_total)

        # Real + Apps (explicada)
        if explained_total is not None:
            self.explained_accum += float(explained_total)

        # Guardar series
        self.calc_series.append(round(self.calc_accum, 2))
        self.real_series.append(round(self.real_accum, 2))
        self.explained_series.append(round(self.explained_accum, 2))

    def get_series(self):
        return {
            "calc": self.calc_series,
            "real": self.real_series,
            "explained": self.explained_series
        }