from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import collection, questions, report

app = FastAPI()

app.include_router(collection.router, tags=['collection'], prefix='/collection')
app.include_router(questions.router, tags=['questions'], prefix='/questions')
app.include_router(report.router, tags=['report'], prefix='/report')