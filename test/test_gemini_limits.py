from google import genai
import os
from dotenv import load_dotenv
load_dotenv()

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
model = client.models.get(model="models/gemini-2.5-flash")
print(model)