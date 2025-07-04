import os
import re
import json
import time
import tempfile
import pandas as pd
from typing import Optional
from PyPDF2 import PdfReader
from google.cloud import storage
from vertexai.generative_models import GenerativeModel
import random


class GeminiExtractor:
    def __init__(
        self,
        bucket_name: Optional[str],
        gcs_prefix: Optional[str],
        pdf_dir: Optional[str],
        output_csv: Optional[str],
        output_json: Optional[str],
        model_name: str = "gemini-2.5-pro",
        max_retries: int = 3,
        retry_delay: float = 3.0,
    ):
        self.pdf_dir = pdf_dir
        self.bucket_name = bucket_name
        self.gcs_prefix = gcs_prefix
        self.output_csv = output_csv
        self.output_json = output_json
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.model = GenerativeModel(model_name)
        self.prompts = self._init_prompts()
        self.records = []
        self.storage_client = storage.Client() if bucket_name else None

    def _init_prompts(self) -> dict:
        return {
            "ThirdPartyServiceProvider": "List the third-party service providers. Return a plain list of names. Do not include markdown formatting or explanations. Return null if none.",
            "SOC1ReportType": "Return the SOC 1 report type as plain text: either Type 1 or Type 2. Do not include markdown formatting or explanations. Return null if none.",
            "ServiceAuditor": "Return the name of the service auditor firm. Plain text only. Do not include markdown formatting or explanations. Return null if none.",
            "AuditorOpinionDate": "Return the auditor’s opinion date in YYYY-MM-DD format. Just the date. Do not include markdown formatting or explanations. Return null if none.",
            "AuditorOpinionType": "Return only the auditor’s opinion as a paragraph. Do not include markdown formatting or explanations. Return null if none.",
            "ReportPeriod": "Return the report period in the format YYYY-MM-DD to YYYY-MM-DD. Just the period. Do not include markdown formatting or explanations. Return null if none.",
            "ServicesProvided": "Return a list of services provided. Each item should be an object with 'service' and 'description' keys. Do not include markdown formatting or explanations. Return null if none.",
            "ReportsInScope": "List the reports provided by the third party service providers. Each item should contain 'report_name', 'source_page', and 'source_control'. Do not include markdown formatting or explanations. Return null if none.",
            "ReportsOutOfScope": "Return a plain list of out-of-scope report names. Do not include markdown formatting or explanations. Return null if none.",
            "ControlObjective": "Return a list of control objectives, each with 'id' and 'objective' fields. Do not include markdown formatting or explanations. Return null if none.",
            "ControlExceptionIdentified": "Return a list of control exceptions as objects with 'control' and 'exception_found'. Do not include markdown formatting or explanations. Return null if none.",
            "ControlNumber": "Return a plain list of control numbers. Do not include markdown formatting or explanations. Return null if none.",
            "ControlDescription": "Return a list of objects with 'number' and 'description' of controls. Do not include markdown formatting or explanations. Return null if none.",
            "CUECNumber": "Return a plain list of CUEC numbers. Do not include markdown formatting or explanations. Return null if none.",
            "CUECDescription": "Return a list of CUECs. Each should have 'number' and 'description'. Do not include markdown formatting or explanations. Return null if none.",
            "SubserviceProvider": "List the subservice providers used. Return a plain list of names. Do not include markdown formatting or explanations. Return null if none."
        }


    def _extract_pdf_text(self, filepath: str) -> str:
        reader = PdfReader(filepath)
        return "\n".join(page.extract_text() or "" for page in reader.pages)

    def _clean_response(self, text: str) -> str:
        """Strip markdown triple backticks and optional json/lang identifiers."""
        cleaned = re.sub(r"```(?:json)?\s*", "", text.strip(), flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned.strip())
        return cleaned.strip()

    def _parse_clean_json_field(self, raw_text: str, field: str) -> Optional[object]:
        """Parse expected JSON fields or fallback to post-processed list or string."""
        cleaned = self._clean_response(raw_text)
        if cleaned.lower() == "null":
            return None

        structured_fields = {
            "ServicesProvided", "ReportsInScope", "ControlObjective",
            "ControlExceptionIdentified", "ControlDescription", "CUECDescription"
        }

        list_like_fields = {
            "ThirdPartyServiceProvider", "ReportsOutOfScope",
            "ControlNumber", "CUECNumber", "SubserviceProvider"
        }

        if field in structured_fields:
            try:
                return json.loads(cleaned)
            except json.JSONDecodeError as e:
                print(f"Warning: Failed to parse field '{field}' as JSON: {e}")
                return cleaned  # fallback as string

        if field in list_like_fields:
            # Normalize newlines, asterisks, and bullets
            items = re.split(r"[\n•\*\-]+", cleaned)
            stripped = [item.strip(" \n\t") for item in items if item.strip()]
            return stripped if stripped else None

        return cleaned

    def _extract_field(self, prompt: str, context: str) -> Optional[str]:
        for attempt in range(self.max_retries):
            try:
                response = self.model.generate_content([context, prompt])
                return response.text.strip()
            except Exception as e:
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt) + random.uniform(0, 1)
                    print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
                else:
                    return f"Error: {str(e)}"
        return None
    
    def _get_pdf_files(self):
        if self.pdf_dir:
            for file in os.listdir(self.pdf_dir):
                if file.endswith(".pdf"):
                    yield os.path.join(self.pdf_dir, file)
        elif self.bucket_name:
            blobs = self.storage_client.list_blobs(self.bucket_name, prefix=self.gcs_prefix)
            for blob in blobs:
                if blob.name.endswith(".pdf"):
                    original_name = os.path.basename(blob.name)
                    tmp_path = os.path.join(tempfile.gettempdir(), original_name)
                    blob.download_to_filename(tmp_path)
                    yield tmp_path

    def process_pdfs(self):
        for filepath in self._get_pdf_files():
            filename = os.path.basename(filepath)
            print(f"\nProcessing: {filename}")
            try:
                full_text = self._extract_pdf_text(filepath)
            except Exception as e:
                print(f"Failed to read {filename}: {e}")
                continue
            result = {"file_name": filename}
            for field, prompt in self.prompts.items():
                print(f" - Extracting: {field}")
                raw_output = self._extract_field(prompt, full_text)
                parsed_output = self._parse_clean_json_field(raw_output, field)
                result[field] = parsed_output
            self.records.append(result)
        return self.records

    def export_to_csv(self):
        df = pd.DataFrame(self.records)
        df.to_csv(self.output_csv, index=False)
        print(f"\nCSV saved to {self.output_csv}")

    def export_to_json(self):
        with open(self.output_json, 'w') as f:
            json.dump(self.records, f, indent=2)
        print(f"\nJSON saved to {self.output_json}")

    def run(self):
        self.process_pdfs()
        self.export_to_json()
        #self.export_to_csv()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract SOC report data from PDFs using Gemini.")
    parser.add_argument("--bucket_name", type=str, help="GCS bucket name containing PDFs.")
    parser.add_argument("--gcs_prefix", type=str, help="Prefix/path in the GCS bucket.")
    parser.add_argument("--pdf_dir", type=str, required=False, help="Directory containing PDF files.")
    parser.add_argument("--output_csv", type=str, required=True, help="Output CSV file path.")
    parser.add_argument("--output_json", type=str, default="gemini_output.json", help="Output JSON file path.")
    parser.add_argument("--model_name", type=str, default="gemini-2.5-pro", help="Gemini model name.")
    parser.add_argument("--max_retries", type=int, default=5, help="Maximum retries for API calls.")
    parser.add_argument("--retry_delay", type=float, default=3.0, help="Delay between retries in seconds.")

    args = parser.parse_args()

    extractor = GeminiExtractor(
        bucket_name=args.bucket_name,
        gcs_prefix=args.gcs_prefix,
        pdf_dir=args.pdf_dir,
        output_csv=args.output_csv,
        output_json=args.output_json,
        model_name=args.model_name,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay
    )
    
    extractor.run()