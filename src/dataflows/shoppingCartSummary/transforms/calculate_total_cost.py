from apache_beam import DoFn, PTransform
import apache_beam as beam

import logging


class CalculateTotalCost(PTransform):
    def expand(self, pcoll):
        # TODO
        logging.info("Calculate total cost of input")
