from apache_beam import DoFn, PTransform
import apache_beam as beam

import logging


class ParsePubsubMsg(PTransform):
    def expand(self, pcoll):
        valid_events, invalid_events = (
                pcoll
                | "Parse JSON" >> beam.ParDo(self.parse_pubsub_json).with_outputs("error", main="elements")
        )
        return valid_events, invalid_events

    def parse_pubsub_json(self, element, created_timestamp=DoFn.TimestampParam):
        # TODO
        logging.info("Parse pubsub message")
