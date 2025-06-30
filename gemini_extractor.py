import os
import json
import time
import glob
import tempfile
import pandas as pd
from typing import Optional
from PyPDF2 import PdfReader
from google.api_core.exceptions import ResourceExhausted
from vertexai.generative_models import GenerativeModel
import random
from google.cloud import storage


class GeminiExtractor:
    def __init__(
        self,
        bucket_name: Optional[str],
        gcs_prefix: Optional[str],
        pdf_dir: Optional[str],
        output_csv: Optional[str],
        output_jsonl: Optional[str],
        model_name: str = "gemini-2.5-pro",
        max_retries: int = 3,
        retry_delay: float = 3.0,
    ):
        self.pdf_dir = pdf_dir
        self.bucket_name = bucket_name
        self.gcs_prefix = gcs_prefix
        self.output_csv = output_csv
        self.output_jsonl = output_jsonl
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.model = GenerativeModel(model_name)
        self.prompts = self._init_prompts()
        self.records = []
        self.storage_client = storage.Client() if bucket_name else None

    def _init_prompts(self) -> dict:
        return {
            "ThirdPartyServiceProvider": "Return the third-party service provider name as a string. Return only the name. If not found, return null. Do not explain.",
            "SOC1ReportType": "Return 'Type 1' or 'Type 2' as a string. Return only the type. If not found, return null. Do not explain.",
            "ServiceAuditor": "Return the service auditor firm name as a string. If not found, return null. No extra text.",
            "AuditorOpinionDate": "Return the auditor opinion date in YYYY-MM-DD format. Return only the date. If not found, return null.",
            "AuditorOpinionType": "Return only the opinion type (e.g. 'unqualified', 'qualified'). If not found, return null. Do not include reasoning.",
            "ReportPeriod": "Return the report period in the format 'YYYY-MM-DD to YYYY-MM-DD'. If not found, return null.",
            "ServicesProvided": "Return a JSON array of services provided. If not found, return null.",
            "ReportsInScope": "Return a JSON array of reports included in scope. If none, return null.",
            "ReportsOutOfScope": "Return a JSON array of excluded reports. If none, return null.",
            "ControlObjective": "Return a JSON array of control objectives. If none found, return null.",
            "ControlExceptionIdentified": "Return a JSON array like [{\"control\": \"CO1\", \"exception_found\": \"No\"}]. If none, return null.",
            "ControlNumber": "Return a JSON array of control numbers. If not found, return null.",
            "ControlDescription": "Return a JSON array of control descriptions: [{\"number\": \"CO1.1\", \"description\": \"...\"}]. If none, return null.",
            "CUECNumber": "Return a JSON array of CUEC numbers. If none, return null.",
            "CUECDescription": "Return a JSON array of CUEC details like [{\"number\": \"CUEC-1\", \"description\": \"...\"}]. If none, return null.",
            "SubserviceProvider": "Return a JSON array of subservice providers. Return names only. If none, return null."
        }

    def _extract_pdf_text(self, filepath: str) -> str:
        reader = PdfReader(filepath)
        return "\n".join(page.extract_text() or "" for page in reader.pages)

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
                result[field] = self._extract_field(prompt, full_text)
            self.records.append(result)
        return self.records

    def export_to_csv(self):
        df = pd.DataFrame(self.records)
        df.to_csv(self.output_csv, index=False)
        print(f"\nCSV saved to {self.output_csv}")

    def export_to_jsonl(self):
        with open(self.output_jsonl, 'w') as f:
            for record in self.records:
                json.dump(record, f)
                f.write('\n')
        print(f"\nJSONL saved to {self.output_jsonl}")

    def run(self):
        self.process_pdfs()
        self.export_to_jsonl()
        self.export_to_csv()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract SOC report data from PDFs using Gemini.")
    parser.add_argument("--bucket_name", type=str, help="GCS bucket name containing PDFs.")
    parser.add_argument("--gcs_prefix", type=str, help="Prefix/path in the GCS bucket.")
    parser.add_argument("--pdf_dir", type=str, required=False, help="Directory containing PDF files.")
    parser.add_argument("--output_csv", type=str, required=True, help="Output CSV file path.")
    parser.add_argument("--output_jsonl", type=str, default="gemini_output.jsonl", help="Output JSONL file path.")
    parser.add_argument("--model_name", type=str, default="gemini-2.5-pro", help="Gemini model name.")
    parser.add_argument("--max_retries", type=int, default=5, help="Maximum retries for API calls.")
    parser.add_argument("--retry_delay", type=float, default=3.0, help="Delay between retries in seconds.")

    args = parser.parse_args()

    extractor = GeminiExtractor(
        bucket_name=args.bucket_name,
        gcs_prefix=args.gcs_prefix,
        pdf_dir=args.pdf_dir,
        output_csv=args.output_csv,
        output_jsonl=args.output_jsonl,
        model_name=args.model_name,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay
    )
    
    extractor.run()