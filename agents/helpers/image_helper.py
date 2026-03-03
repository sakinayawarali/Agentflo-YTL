

# from dotenv import load_dotenv
# import requests
# import tempfile
# from groq import Groq
# import os

# load_dotenv()

# class WhatsAppAudioTranscriber:
#     def __init__(self):
#         self.access_token = os.getenv("WHATSAPP_ACCESS_TOKEN")
#         self.phone_number_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
#         self.headers = {'Authorization': f'Bearer {self.access_token}'}
#         self.groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))
        
    
#     def transcribe_audio(self, ogg_file_path):
#         with open(ogg_file_path, "rb") as file:
#             translation = self.groq_client.audio.translations.create(
#                 file=file,
#                 model="whisper-large-v3"
                
#             )
#         return translation.text
    
#     def get_media_url(self, media_id):
#         url = f"https://graph.facebook.com/v23.0/{media_id}"
#         response = requests.get(url, headers=self.headers)
#         return response.json()["url"]
    
#     def download_audio(self, media_url, save_path=None):
#         if not save_path:
#             temp_file = tempfile.NamedTemporaryFile(suffix='.ogg', delete=False)
#             save_path = temp_file.name
#             temp_file.close()
        
#         response = requests.get(media_url, headers=self.headers)
#         with open(save_path, 'wb') as f:
#             f.write(response.content)
#         return save_path

#     def transcribe_whatsapp_vn(self, media_id):
#         media_url = self.get_media_url(media_id)
#         audio_path = self.download_audio(media_url)
#         transcription = self.transcribe_audio(audio_path)
#         os.remove(audio_path)
#         return transcription


# if __name__ == "__main__":
#     transcriber = WhatsAppAudioTranscriber()

#     transcription = transcriber.transcribe_whatsapp_vn(627794643701433)
#     print(transcription)

from typing import List, Optional
from pydantic import BaseModel, Field


class OrderItems(BaseModel):
    sku_code: Optional[str]
    quantity: Optional[int]
    price: Optional[float]
    name: Optional[str]

class ImageOrderDraft(BaseModel):
    items: List[OrderItems]


class InvoiceExtraction(BaseModel):
    tenant_id: str = Field(..., description="The tenant identifier from TENANT_ID env.")
    mobile_number: Optional[str] = Field(None, description="Leave empty; set from WhatsApp sender instead.")
    invoice_numbers: List[str] = Field(..., description="List of invoice numbers found on the document.")
    store_codes: List[str] = Field(..., description="List of customer/store codes (e.g., N00...).")
    delivery_dates: List[str] = Field(..., description="List of delivery dates in YYYY-MM-DD format.")

import os
import tempfile
from dotenv import load_dotenv
from google import genai
from google.genai.types import HttpOptions, Part
import requests


load_dotenv()

class WhatsAppImageHelper:
    DEFAULT_MODEL = "gemini-2.5-flash"
    INVOICE_TENANT_ID = (os.getenv("TENANT_ID") or "").strip()
    INVOICE_SYSTEM_PROMPT = (
        "You are an Invoice Verification AI for the \"SUPER SINDH\" / EBM distribution system.\n"
        "Your task is to analyze the provided invoice image, extract specific metadata, and return ONLY a JSON object\n"
        "that matches the verify_invoice tool arguments.\n\n"
        "Extraction Rules:\n"
        f"1) tenant_id: Always set to \"{INVOICE_TENANT_ID}\".\n"
        "2) invoice_numbers: Look for \"Invoice No\" in the header. The invoice number always contains \"INV\" "
        "in the middle (e.g., D0663INV11816). If OCR returns a slash or missing V (e.g., D0663IN/11816), "
        "normalize it to include \"INV\".\n"
        "3) store_codes: Look for the code before the customer name (e.g., N00000313606).\n"
        "4) delivery_dates: Look for \"Deliv.Date\" and convert from DD-MM-YYYY to YYYY-MM-DD.\n"
        "   Example: 07-01-2026 -> 2026-01-07.\n"
        "5) mobile_number: Do NOT extract from the invoice. Leave it empty if present.\n\n"
        "Output Requirements:\n"
        "Do not output conversational text. Return ONLY the JSON object with keys:\n"
        "tenant_id, mobile_number, invoice_numbers, store_codes, delivery_dates."
    )

    def __init__(self, model_id=DEFAULT_MODEL):
        api_key = (
            os.getenv("GEMINI_API_KEY")
            or os.getenv("GOOGLE_API_KEY")
            or os.getenv("GENAI_API_KEY")
        )
        http_opts = HttpOptions(
            baseUrl="https://generativelanguage.googleapis.com",
            apiVersion="v1beta",
        )
        self.client = genai.Client(api_key=api_key, http_options=http_opts, vertexai=False) if api_key else genai.Client(http_options=http_opts, vertexai=False)
        self.model_id = model_id
        self.invoice_model_id = os.getenv("INVOICE_VISION_MODEL") or self.model_id

    def inference_image(self, prompt, image_path):
        with open(image_path, 'rb') as image_file:
            image_bytes = image_file.read()

        response = self.client.models.generate_content(
            model=self.model_id,
            contents=[prompt, Part.from_bytes(data=image_bytes, mime_type="image/jpeg")]
        )
        return response.text

    def inference_img_struct_output(self,prompt, image_path) -> str:
        with open(image_path, 'rb') as image_file:
            image_bytes = image_file.read()

        response = self.client.models.generate_content(
            model=self.model_id,
            contents=[prompt, Part.from_bytes(data=image_bytes, mime_type="image/jpeg")],
            config={
                'response_mime_type': 'application/json',
                'response_json_schema': ImageOrderDraft.model_json_schema()
            }
        )
        if response.text:
            return response.text
        else:
            raise ValueError("No structured output received from the model.")

    def inference_invoice_struct_output(self, prompt, image_path) -> str:
        with open(image_path, "rb") as image_file:
            image_bytes = image_file.read()

        response = self.client.models.generate_content(
            model=self.invoice_model_id,
            contents=[prompt, Part.from_bytes(data=image_bytes, mime_type="image/jpeg")],
            config={
                "response_mime_type": "application/json",
                "response_json_schema": InvoiceExtraction.model_json_schema(),
            },
        )
        if response.text:
            return response.text
        raise ValueError("No structured output received from the model.")

    def get_media_url(self, media_id):
        url = f"https://graph.facebook.com/v23.0/{media_id}"
        response = requests.get(url, headers={'Authorization': f'Bearer {os.getenv("WHATSAPP_ACCESS_TOKEN")}'})
        return response.json()["url"]
    
    def download_image(self, media_url, save_path=None):
        if not save_path:
            temp_file = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
            save_path = temp_file.name
            temp_file.close()
        
        response = requests.get(media_url, headers={'Authorization': f'Bearer {os.getenv("WHATSAPP_ACCESS_TOKEN")}'})
        with open(save_path, 'wb') as f:
            f.write(response.content)
            
        return save_path

    def inference_whatsapp_image(self, media_id, prompt):
        media_url = self.get_media_url(media_id)
        image_path = self.download_image(media_url)
        analysis = self.inference_image(prompt, image_path)
        os.remove(image_path)
        return analysis
    

    def get_order_from_image(self, media_id):
        prompt = "Extract the order details from this image."
        response = self.inference_whatsapp_image(media_id, prompt)
        return response
    
    
    def get_order_from_image_structured_output(self, media_id) -> str:
        prompt = "Extract the order details from this image."
        media_url = self.get_media_url(media_id)
        image_path = self.download_image(media_url)
        response = self.inference_img_struct_output(prompt, image_path)
        os.remove(image_path)
        return response

    def get_invoice_from_image_structured_output(self, media_id) -> str:
        media_url = self.get_media_url(media_id)
        image_path = self.download_image(media_url)
        response = self.inference_invoice_struct_output(self.INVOICE_SYSTEM_PROMPT, image_path)
        os.remove(image_path)
        return response

    def get_invoice_from_image_path(self, image_path: str) -> str:
        return self.inference_invoice_struct_output(self.INVOICE_SYSTEM_PROMPT, image_path)


if __name__ == "__main__":
    helper = WhatsAppImageHelper()
    media_id = "1473411823565861"
    # analysis = helper.get_order_from_image(media_id)
    analysis = helper.get_order_from_image_structured_output(media_id)
    print(analysis)
    print(type(analysis))
