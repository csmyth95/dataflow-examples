import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from transforms.total_shopping_cart import TotalShoppingCart


class ShoppingCartSummaryOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(self, parser):
        parser.add_argument("--proj")
        parser.add_argument("--topic")


class ShoppingCartSummaryPipeline:
    def __init__(self, project_id, topic, p_begin):
        self.project_id = project_id
        self.topic = topic
        self.p_begin = p_begin
        self.__build_pipeline()

    def __build_pipeline(self):
        src_subscription = f"projects/{self.project_id}/subscriptions/{self.topic}"
        dataset = "shoppingCart"
        table = "summary"

        logging.info(f"Starting Shopping Cart Summary Dataflow pipeline for topic {self.topic}")
        bq_messages, bq_errors = (
            self.p_begin
            | f'Read raw events from {self.topic}' >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription=src_subscription,
                timestamp_attribute="messageTimestamp")
            | 'Total Shopping Cart Items' >> TotalShoppingCart(
                self.project_id, self.topic)
        )

        account_closure_table = f"{self.project_id}:{dataset}.{table}"
        bq_messages | 'Write shopping cart summaries to BigQuery' >> beam.io.WriteToBigQuery(table=account_closure_table)

        errors_table = f"{self.project_id}:{dataset}.errors"
        bq_errors | "Write errors to BigQuery" >> beam.io.WriteToBigQuery(table=errors_table)

    def run(self):
        self.p_begin.run()

    def run_with_wait(self):
        self.p_begin.run().wait_until_finish()


def generate_pipeline():
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args, streaming=True)

    shopping_cart_summary_options = pipeline_options.view_as(ShoppingCartSummaryOptions)
    pipeline = beam.Pipeline(options=pipeline_options)

    project_id = shopping_cart_summary_options.proj
    topic = shopping_cart_summary_options.topic

    topic_pipeline = ShoppingCartSummaryPipeline(
        project_id=project_id,
        topic=topic,
        p_begin=pipeline
    )
    topic_pipeline.run()


if __name__ == '__main__':
    generate_pipeline()
