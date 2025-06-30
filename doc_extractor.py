import logging
from google.cloud import documentai_v1beta3 as documentai
from google.api_core.client_options import ClientOptions
from google.cloud import storage
import os
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

storage_client = storage.Client()


def is_valid_file(filename):
    return filename.lower().endswith(".pdf")

class DocAIExtractor:
    def __init__(self, project_id, location, processor_id, bucket_name, doc_input_prefix, doc_output_prefix, output_jsonl,
                 batch_limit=20, field_mask="text,entities"):
        self.project_id = project_id
        self.location = location
        self.processor_id = processor_id
        self.bucket_name = bucket_name
        self.doc_input_prefix = doc_input_prefix
        self.doc_output_prefix = doc_output_prefix
        self.batch_limit = batch_limit
        self.field_mask = field_mask
        self.output_jsonl = output_jsonl
        self.docai_client = documentai.DocumentProcessorServiceClient(
            client_options=ClientOptions(api_endpoint=f"{self.location}-documentai.googleapis.com")
        )
        self.records = []

    def submit_batch_docai_job(self):
        all_blobs = list(storage_client.list_blobs(self.bucket_name, prefix=self.doc_input_prefix))
        file_uris = [
            f"gs://{self.bucket_name}/{blob.name}"
            for blob in all_blobs
            if not blob.name.endswith("/") and is_valid_file(blob.name)
        ]

        logging.info(f"Found {len(file_uris)} valid files for processing.")
        logging.info(f"Processing files in batches of {self.batch_limit}...")

        for i in range(0, len(file_uris), self.batch_limit):
            chunk = file_uris[i:i + self.batch_limit]

            gcs_documents = [
                documentai.GcsDocument(gcs_uri=uri, mime_type="application/pdf") for uri in chunk
            ]

            input_config = documentai.BatchDocumentsInputConfig(
                gcs_documents=documentai.GcsDocuments(documents=gcs_documents)
            )

            gcs_output_config = documentai.DocumentOutputConfig.GcsOutputConfig(
                gcs_uri=f"gs://{self.bucket_name}/{self.doc_output_prefix}",
                field_mask=self.field_mask
            )

            output_config = documentai.DocumentOutputConfig(
                gcs_output_config=gcs_output_config
            )

            processor_path = self.docai_client.processor_path(
                self.project_id, self.location, self.processor_id
            )

            request = documentai.BatchProcessRequest(
                name=processor_path,
                input_documents=input_config,
                document_output_config=output_config
            )

            operation = self.docai_client.batch_process_documents(request=request)
            logging.info(f"Waiting for batch operation {operation.operation.name} to complete...")
            operation.result()
            logging.info(f"Batch {i // self.batch_limit + 1} complete.")


    def parse_docai_results(self):
        output_blobs = storage_client.list_blobs(self.bucket_name, prefix=self.doc_output_prefix)

        for blob in output_blobs:
            if not blob.name.endswith(".json"):
                continue

            logging.info(f"Processing output: {blob.name}")
            document = documentai.Document.from_json(blob.download_as_bytes(), ignore_unknown_fields=True)
            entity_dict = {e.type_: e.mention_text.strip() for e in document.entities}

            filename = os.path.basename(blob.name)
            entity_dict["file_name"] = filename

            self.records.append(entity_dict)
            logging.info(f"Extracted entities from {filename}: {entity_dict}")
        return self.records

    def export_to_jsonl(self):
        with open(self.output_jsonl, 'w') as f:
            for record in self.records:
                json.dump(record, f)
                f.write('\n')
        print(f"\nJSONL saved to {self.output_jsonl}")

    def run(self):
        logging.info("Starting Document AI batch processing...")
        self.submit_batch_docai_job()
        logging.info("Document AI batch processing completed.")
        logging.info("Parsing Document AI results...")
        results = self.parse_docai_results()
        logging.info("Document AI results parsing completed.")
        self.export_to_jsonl()
        return results

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract data from PDFs using Google Document AI.")
    parser.add_argument("--project_id", type=str, required=True, help="Google Cloud project ID.")
    parser.add_argument("--location", type=str, default="us", help="Document AI location (default: us).")
    parser.add_argument("--processor_id", type=str, required=True, help="Document AI processor ID.")
    parser.add_argument("--bucket_name", type=str, required=True, help="Google Cloud Storage bucket name.")
    parser.add_argument("--doc_input_prefix", type=str, required=True, help="Input prefix for documents in GCS.")
    parser.add_argument("--doc_output_prefix", type=str, required=True, help="Output prefix for processed documents in GCS.")
    parser.add_argument("--batch_limit", type=int, default=20, help="Number of files to process in each batch (default: 20).")
    parser.add_argument("--field_mask", type=str, default=None, help="Field mask for output documents.")
    parser.add_argument("--output_jsonl", type=str, default="docai_output.jsonl", help="Output JSONL file path.")

    args = parser.parse_args()

    extractor = DocAIExtractor(
        project_id=args.project_id,
        location=args.location,
        processor_id=args.processor_id,
        bucket_name=args.bucket_name,
        doc_input_prefix=args.doc_input_prefix,
        doc_output_prefix=args.doc_output_prefix,
        output_jsonl=args.output_jsonl,
        batch_limit=args.batch_limit,
        field_mask=args.field_mask 
    )
    
    extractor.run()