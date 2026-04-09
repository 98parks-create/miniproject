import os
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()
api_key = os.environ.get("GOOGLE_API_KEY")

print(f"Testing API Key: {api_key[:10]}...")

try:
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.0-flash')
    response = model.generate_content("Hello, this is a test.")
    print("SUCCESS: Response received!")
    print(response.text)
except Exception as e:
    print(f"FAILED: Error occurred: {str(e)}")
