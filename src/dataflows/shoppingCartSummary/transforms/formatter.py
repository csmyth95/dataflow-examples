import logging

import apache_beam as beam
from apache_beam import PTransform


class FormatBigQueryMessage(PTransform):
    def __init__(self, timestamp_fields):
        self.timestamp_fields = ['_TIMESTAMP', 'createdTimestamp'] + timestamp_fields

    def expand(self, pcoll):
        valid_bq_msgs, invalid_events = (
            pcoll
            | "Format message for BigQuery" >> beam.ParDo(self.format_message).with_outputs("error", main="elements")
        )
        return valid_bq_msgs, invalid_events

    def format_message(self, original_element):
        logging.info("Formatting message for BQ....")


class FormatProcessingError(PTransform):
    def __init__(self, topic):
        self.topic = topic

    def expand(self, pcoll):
        return pcoll | "Format Processing Error" >> beam.Map(self.format_error)

    def format_error(self, element):
        logging.info("Formatting error...")
        # TODO Finish method
