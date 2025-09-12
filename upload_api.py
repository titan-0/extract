from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
import os
from test_import import runn

app = FastAPI()
UPLOAD_DIR = "test_files"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.post("/upload")
def upload_file(file: UploadFile = File(...)):
    file_location = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_location, "wb") as f:
        f.write(file.file.read())
    runn()
    return JSONResponse({"filename": file.filename, "saved_to": file_location})

# To run: uvicorn upload_api:app --reload
# You can POST files to /upload from your frontend using FormData.
