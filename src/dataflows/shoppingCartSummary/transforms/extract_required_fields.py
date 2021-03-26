from apache_beam import PTransform
import logging


class ExtractRequiredFields(PTransform):
    def __init__(self, fields):
        self.fields = fields

    def expand(self, pcoll):
        logging.info("Extracting required fields for sinks...")
        # TODO