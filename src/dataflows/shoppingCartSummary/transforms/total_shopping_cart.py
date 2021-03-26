from apache_beam import PTransform
import apache_beam as beam

from transforms.parse_pubsub_msg import ParsePubsubMsg
from transforms.calculate_total_cost import CalculateTotalCost
from transforms.formatter import FormatProcessingError
from transforms.formatter import FormatBigQueryMessage
from transforms.extract_required_fields import ExtractRequiredFields


class TotalShoppingCart(PTransform):
    def __init__(self, project_id, topic):
        super().__init__()
        self.project_id = project_id
        self.topic = topic

    def expand(self, sources):
        valid_events, invalid_events = (sources | 'Parse Pubsub message' >> ParsePubsubMsg(self.topic))

        total_costs, cost_errors = (
           valid_events
           | 'Call exclusion APIs' >> CalculateTotalCost()
        )

        required_bq_fields = ["created", "messageString", "totalCost"]  # TODO UPDATE with more fields
        formatted_total_costs = (
                total_costs
                | f"Extract required fields for parsed {self.topic}" >>
                ExtractRequiredFields(fields=required_bq_fields)
        )

        formatted_bq_messages, bq_msg_errors = (
            formatted_total_costs | 'Format message for BigQuery' >> FormatBigQueryMessage())

        formatted_bq_errors = (
                (invalid_events, cost_errors, bq_msg_errors)
                | f"Combine all items for {self.topic}" >> beam.Flatten()
                | f'Format errors from {self.topic}' >> FormatProcessingError(topic=self.topic))

        return formatted_bq_messages, formatted_bq_errors
