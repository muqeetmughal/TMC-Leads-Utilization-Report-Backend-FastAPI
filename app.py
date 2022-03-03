from fastapi import FastAPI
from pydantic import BaseModel
from main import LeadUtilizationReport
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
script = LeadUtilizationReport()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Data(BaseModel):
    server_id: int
    list_id: int


@app.get("/list-servers")
def list_servers():

    # print(script.list_servers())
    return script.list_servers()


@app.get("/list-ids/{server_id}")
def list_ids_of_server(server_id: int):
    return script.select_server(server_id)


@app.post("/report")
def details(input: Data):
    output = script.single_select(
        list_id=input.list_id, server_id=input.server_id)
    print(output)
    return output
